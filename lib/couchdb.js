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
            var filter = opts.data || {};
            var sort = filter[ "$sort" ];
            var skip = filter[ "$skip" ];
            var limit = filter[ "$limit" ];
            delete filter[ "$sort" ];
            delete filter[ "$skip" ];
            delete filter[ "$limit" ];

            var qs = {};
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

            qs = querystring.stringify( _.extend( qs, opts.querystring || {} ) );
            var url = opts.url + "/_all_docs?include_docs=true&" + qs;
            req({ url: url }, function( err, res, body ) {
                if ( err ) {
                    return ( err.name != "NotFoundError" ) ? cb( err ) : cb( null, [] );
                }

                var results = body.rows.map( function( row ) {
                    var doc = row.doc;
                    doc.id = row.id;
                    doc.rev = row.value.rev;
                    delete doc._id;
                    delete doc._rev;
                    return doc;
                });

                var count = results.length;
                var last_id = count ? results[ count - 1 ].id : null

                results = sift( filter, results ).reverse(); // sift reverses the order
                if ( sort ) {
                    results = _.sortBy( results, sort );
                }
                if ( skip ) {
                    results.splice( 0, skip );
                }
                if ( limit ) {
                    results.splice( limit );
                }

                if ( opts.info ) {

                    if ( results.length ) {
                        last_id = _.max( results, function ( r ) {
                            return r.id
                        }).id;
                    }

                    results = {
                        results: results,
                        count: count,
                        last_id: last_id,
                        offset: body.offset,
                        total: body.total_rows,
                    }
                }

                cb( null, results );
            });
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