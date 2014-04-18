var crypto = require( "crypto" );
var querystring = require( "querystring" );

var request = require( "request" );
var uuid = require( "node-uuid" );
var _ = require( "underscore" );
var sift = require( "sift" );

var backsync = require( ".." );

module.exports = function( settings ) {
    settings = _.extend({
        max_requests: Infinity,
        limit_per_request: Infinity,
        hardlimit: 5000,
        qs: {},
    }, settings );

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
            var s = _.extend({}, settings, ( opts.backsync || {} ).couchdb );

            var filter = _.clone( opts.data || {} );
            var sort = filter[ "$sort" ];
            var skip = filter[ "$skip" ] || 0;
            var limit = filter[ "$limit" ] || Infinity;
            delete filter[ "$sort" ];
            delete filter[ "$skip" ];
            delete filter[ "$limit" ];

            // build the query string
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

            if ( s.request_limit && s.request_limit != Infinity ) {
                qs[ "limit" ] = s.request_limit;
            }

            var results = [];
            var last;
            var info = { requests: 0, scanned: 0 };
            var next = function( url, qs ) {
                var _qs = _.extend( qs, s.qs || {} );
                var _url = url + "/_all_docs?" + querystring.stringify( _qs );
                req({ url: _url }, function( err, res, body ) {
                    if ( err && err.name != "NotFoundError" ) return cb( err );

                    // skip the first row if it was already processed
                    var _results = body.rows || [];
                    if ( last && _results[ 0 ] && _results[ 0 ].id == last.id ) {
                        _results.splice( 0, 1 );
                    }

                    for ( var i = 0 ; i < _results.length ; i += 1 ) {
                        var row = _results[ i ];
                        row.doc.id = row.id;
                        row.doc.rev = row.value.rev;
                        delete row.doc._id;
                        delete row.doc._rev;
                        _results[ i ] = row.doc;
                    }

                    // notify listeners of this request
                    info.requests += 1;
                    info.scanned += _results.length;
                    _.extend( info, { url: _url, body: body });
                    collection.trigger( "request", collection, info, opts );

                    // prepare the start key of the next iteration
                    last = _results[ _results.length - 1 ];
                    qs[ "startkey" ] = ( last ) ? last.id : null;

                    // filter the results
                    _results = sift( filter, _results ).reverse(); // sift reverses the order

                    // append the batch results into the total results
                    results.push.apply( results, _results );

                    // determine if we're done or we should keep iterating
                    // to the next batch
                    var isdone = !body.rows.length || --s.max_requests == 0;

                    // sort the results before limit and skip, only after all
                    // of the results has been collected
                    if ( isdone && sort ) {
                        results = _.sortBy( results, sort );
                    }

                    // apply the skip and limit, when we're done collecting all
                    // of the results. alternatively, if there's no sort, we can
                    // apply these at every iteration as a memory optimization
                    if ( !sort || isdone ) {
                        if ( skip > 0 ) {
                            var skipped = results.splice( 0, skip );
                            skip -= skipped.length;
                        }

                        if ( skip <= 0 && limit >= 0 ) {
                            var truncated = results.splice( limit );
                            limit -= results.length ? limit : truncated.length;
                        }

                        // we're done when all of the limit was consumed
                        isdone = isdone || limit <= 0;
                    }

                    if ( !isdone ) return next( url, qs ); // keep going
                    cb( null, results ); // we're done!
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