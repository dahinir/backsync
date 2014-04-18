var _ = require( "underscore" );
var uuid = require( "node-uuid" );

var urlError = function() {
    throw new Error( "A \"url\" property or function must be specified" );
};

module.exports = function() {
    var fns = {};
    var sync = function( method, model, options ) {
        options || ( options = {} );
        if ( !options.url ) {
            options.url = _.result( model, "url" ) || urlError();
        }

        if ( method == "read" && Array.isArray( model.models ) ) {
            method = "search";
        }

        // break down the url into a base and an id
        var base = options.url.split( "/" );
        if ( [ "read", "update", "patch", "delete" ].indexOf( method ) != -1 ) {
            options.url_id = base.pop();
            if ( !options.url_id || encodeURIComponent( model.id ) != options.url_id ) {
                options.error( new Error( "Missing or incorrect Model id" ) )
                return
            }
        }

        options.url_base = base.join( "/" );
        var that = this;
        var cb = function( err, res ) {
            ( err ) ? options.error( err ) : options.success( res );
        };

        var fn = fns[ method ];
        if ( !fn ) throw new Error( "backsync '" + method + "' is not implemented" );
        process.nextTick(function() {
            fn.call( that, model, options, cb );
        });
    };

    var setfn = function( name ) {
        return function( fn ) {
            fns[ name ] = fn;
            return this;
        }
    }

    sync.create = sync.insert = setfn( "create" );
    sync.read = setfn( "read" );
    sync.update = setfn( "update" );
    sync.patch = setfn( "patch" );
    sync.search = sync.list = setfn( "search" );
    sync.delete = sync.remove = setfn( "delete" );
    return sync;
};

module.exports.NotFoundError = function( id ) {
    this.name = "NotFoundError";
    this.message = "'" + id + "' is not found";
};
module.exports.NotFoundError.prototype = Error.prototype;

module.exports.memory = require( "./memory" );
module.exports.mongodb = require( "./mongodb" );
module.exports.couchdb = require( "./couchdb" );