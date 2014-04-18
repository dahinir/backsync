var crypto = require( "crypto" );
var querystring = require( "querystring" );

var request = require( "request" );
var uuid = require( "node-uuid" );
var _ = require( "underscore" );
var sift = require( "sift" );

var backsync = require( ".." );

module.exports = function( settings ) {
    settings || ( settings = {} );
    var _request = settings.request || request; // dependency injection

    var req = function( opts, cb ) {
        if ( settings.dsn && opts.url.indexOf( "http://" ) != 0 ) {
            if ( opts.url[ 0 ] == "/" ) {
                opts.url = "_" + opts.url.substr( 1 );
            }
            opts.url = settings.dsn + opts.url;
        }
        _request( opts, function( err, res, body ) {
            if ( err ) return cb( err, res, body );
            try {
                var _body = JSON.parse( body );
            } catch ( err ) {
                return cb( err, res, body );
            }

            if ( _body[ "error" ] ) {
                if ( _body[ "error" ] == "not_found" ) {
                    _body.name = "NotFoundError";
                }
                err = new Error( _body[ "error" ] );
                delete _body[ "error" ];
                return cb( _.extend( err, _body ), res, body )
            }

            cb( null, res, _body );
        });
    };

    return backsync()
        .search( function( collection, opts, cb ) {
            var filter = _.clone( opts.data || {} );
            var sort = filter[ "$sort" ];
            var skip = filter[ "$skip" ] || 0;
            var limit = filter[ "$limit" ] || Infinity;
            delete filter[ "$sort" ];
            delete filter[ "$skip" ];
            delete filter[ "$limit" ];

            var qs = { include_docs: true };
            if ( filter.id && typeof filter.id == "object" ) {
                var in_ = filter.id.$in;
                if ( typeof in_ != "undefined" && Array.isArray( in_ ) ) {
                    qs[ "keys" ] = JSON.stringify( in_ );
                }

                var startkey = filter.id.$gte || filter.id.$gt;
                if ( typeof startkey != "undefined" ) {
                    qs[ "startkey" ] = JSON.stringify( startkey );
                }

                var endkey = filter.id.$lte || filter.id.$lt;
                if ( typeof endkey != "undefined" ) {
                    qs[ "endkey" ] = JSON.stringify( endkey );
                    if ( typeof filter.id.$lte == "undefined" ) {
                        qs[ "inclusive_end" ] = false;
                    }
                }
            }

            var results = [];
            var scanned = 0;
            var requests = 0;
            var last;
            var offset;
            var next = function( url, qs ) {
                var _qs = _.extend( qs, opts.querystring || {} );
                var _url = url + "/_all_docs?" + querystring.stringify( _qs );
                req({ url: _url }, function( err, res, body ) {
                    requests += 1;
                    if ( err && err.name != "NotFoundError" ) return cb( err );
                    var _results = body.rows || [];
                    var remains = body.total_rows - body.offset - _results.length;
                    if ( typeof offset == "undefined" ) offset = body.offset;

                    // skip the first row if it was already processed
                    if ( last && _results[ 0 ] && _results[ 0 ].id == last.id ) {
                        _results.splice( 0, 1 );
                    }

                    scanned += _results.length;
                    for ( var i = 0 ; i < _results.length ; i += 1 ) {
                        var row = _results[ i ];
                        row.doc.id = row.id;
                        row.doc.rev = row.value.rev;
                        delete row.doc._id;
                        delete row.doc._rev;
                        _results[ i ] = row.doc;
                    }

                    last = _results[ _results.length - 1 ];
                    _results = sift( filter, _results ).reverse(); // sift reverses the order
                    results.push.apply( results, _results );

                    // keep iterating
                    qs[ "startkey" ] = ( last ) ? last.id : null;
                    if ( sort && remains >= 0 && body.rows.length ) {
                        // we must collect all of the results for sorting
                        return next( url, qs );
                    }

                    _results = _.sortBy( results, sort ).slice( skip )
                    if ( _results.length < limit && remains >= 0 && body.rows.length ) {
                        next( url, qs );
                    } else {
                        _results.splice( limit );
                        if ( opts.info ) {
                            _results = {
                                results: _results,
                                total: body.total_rows,
                                offset: offset,
                                scanned: scanned,
                                requests: requests
                            }
                        }
                        cb( null, _results );
                    }
                });
            };

            next( opts.url, qs );
        })
        .delete( function( model, opts, cb ) {
            if ( !model.get( "rev" ) ) {
                var args = arguments, that = this;
                return req({ url: opts.url }, function( err, res, body ) {
                    if ( err ) return cb( err );
                    model.set( "rev", body._rev );
                    if ( !model.get( "rev" ) ) {
                        return cb( new Error( "No 'rev' found" ) );
                    }
                    args.callee.apply( that, args );
                });
            }

            var url = opts.url + "?rev=" + model.get( "rev" );
            req({ url: url, method: "DELETE" }, function( err, res, body ) {
                if ( err ) return cb( err );
                cb();
            })
        })
        .patch( function( model, opts, cb ) {
            req({ url: opts.url }, function( err, res, body ) {
                if ( err ) return cb( err );
                var rev = body._rev;
                delete body._rev;
                delete body._id;
                _.extend( body, opts.attrs );

                var url = opts.url + "?rev=" + rev;
                var _body = JSON.stringify( body );
                req({ url: url, method: "PUT", body: _body }, function( err, res, _body ) {
                    if ( err ) return cb( err );
                    body.rev = _body.rev;
                    cb( null, body );
                })
            });
        })
        .read( function( model, opts, cb ) {
            req({ url: opts.url }, function( err, res, body ) {
                if ( err ) return cb( err );
                body.id = body._id;
                body.rev = body._rev;
                delete body._id;
                delete body._rev;
                cb( null, body );
            });
        })
        .update( function( model, opts, cb ) {
            var that = this;
            var args = arguments;
            var data = model.toJSON();

            var url = opts.url;
            var rev = model.get( "rev" );
            if ( rev ) {
                url += "?rev=" + model.get( "rev" );
            }

            var create_db = opts.create_db;
            if ( typeof create_db == "undefined" ) {
                create_db = settings.create_db;
            }

            req( {
                url: url,
                method: "PUT",
                body: JSON.stringify( data )
            }, function( err, res, body ) {
                if ( err && err.reason == "no_db_file" && create_db ) {
                    req({ url: opts.url_base, method: "PUT" }, function( err, res, body ) {
                        if ( err && err.message != "file_exists" ) return cb( err );
                        opts.create_db = false; // avoid infinite loop
                        args.callee.apply( that, args );
                    });
                    return;
                } else if ( err && err.message == "conflict" ) {
                    req({ url: url }, function( err, res, body ) {
                        if ( err ) return cb( err );
                        model.set( "rev", body._rev );
                        args.callee.apply( that, args );
                    });
                    return;
                }
                if ( err ) return cb( err );
                delete body.ok;
                cb( null, body );
            });
        })
        .create( function( model, opts, cb ) {
            var that = this;
            var args = arguments;
            var id = new Date().getTime().toString( 16 );
            id = crypto.createHash( "md5" )
                .update( uuid.v4() )
                .digest( "hex" )
                .substr( 0, 32 - id.length ) + id
            var url = opts.url_base + "/" + id;

            var create_db = opts.create_db;
            if ( typeof create_db == "undefined" ) {
                create_db = settings.create_db;
            }

            var data = model.toJSON();
            req( {
                url: url,
                method: "PUT",
                body: JSON.stringify( data )
            }, function( err, res, body ) {
                if ( err && err.reason == "no_db_file" && create_db ) {
                    req({ url: opts.url_base, method: "PUT" }, function( err, res, body ) {
                        if ( err && err.message != "file_exists" ) return cb( err );
                        opts.create_db = false; // avoid infinite loop
                        args.callee.apply( that, args );
                    });
                    return;
                }
                if ( err ) return cb( err );

                delete body.ok;
                cb( null, _.extend( data, body ) );
            })

        });

}