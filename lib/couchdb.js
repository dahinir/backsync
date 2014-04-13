var crypto = require( "crypto" );

var request = require( "request" );
var uuid = require( "node-uuid" );
var _ = require( "underscore" );

var backsync = require( ".." );

module.exports = function( settings ) {

    var req = function( opts, cb ) {
        request( opts, function( err, res, body ) {
            if ( err ) return cb( err, res, body );
            try {
                var _body = JSON.parse( body );
            } catch ( err ) {
                return cb( err, res, body );
            }

            if ( _body[ "error" ] ) {
                err = new Error( _body[ "error" ] );
                delete _body[ "error" ];
                return cb( _.extend( err, _body ), res, body )
            }

            cb( null, res, _body );
        });
    };

    return backsync()
        .search( function( collection, opts, cb ) {
            var url = opts.url + "/_all_docs?include_docs=true";
            req({ url: url }, function( err, res, body ) {
                if ( err ) return cb( err );
                var results = [];
                for ( var i = 0 ; i < body.rows.length ; i += 1 ) {
                    var doc = body.rows[ i ].doc;
                    doc.id = doc._id;
                    doc.rev = doc._rev;
                    delete doc._id;
                    delete doc._rev;
                    results.push( doc );
                }
                cb( null, results );
            });
        })
        .delete( function( model, opts, cb ) {
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
                if ( err && err.message == "not_found" ) {
                    err.name = "NotFoundError"
                }

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

            req( {
                url: url,
                method: "PUT",
                body: JSON.stringify( data )
            }, function( err, res, body ) {
                if ( err && err.reason == "no_db_file" && opts.create_db ) {
                    req({ url: opts.url_base, method: "PUT" }, function( err, res, body ) {
                        if ( err && err.message != "file_exists" ) return cb( err );
                        opts.create_db = false; // avoid infinite loop
                        args.callee.apply( that, args );
                    });
                    return;
                }
                if ( err ) return cb( err );
                delete body.ok;
                cb( null, body ); // _.extend( data, body ) );
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

            var data = model.toJSON();
            req( {
                url: url,
                method: "PUT",
                body: JSON.stringify( data )
            }, function( err, res, body ) {
                if ( err && err.reason == "no_db_file" && opts.create_db ) {
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

            // request.put( url, JSON.stringify( model ), function( err, res, body ) {
            //     if ( err ) return cb( err );

            // });

        });

}