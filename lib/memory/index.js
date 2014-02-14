var _ = require( "underscore" );
var uuid = require( "node-uuid" );
var sift = require( "sift" );

var backsync = require( ".." );

var globalstore = {};
module.exports = function( datastore ) {
    var ds = datastore || globalstore;
    return backsync()
        .create( function( model, opts, cb ) {
            var id = uuid.v4();
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            ds[ opts.url_base ][ id ] = _.extend( {}, model.toJSON(), { id: id } );
            cb( null, _( ds[ opts.url_base ][ id ] ).clone() );

        }).update( function( model, opts, cb ) {
            var id = opts.url_id;
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            ds[ opts.url_base ][ id ] = _.extend( {}, model.toJSON(), { id: id } );
            cb( null, _( ds[ opts.url_base ][ id ] ).clone() );

        }).patch( function( model, opts, cb ) {
            var id = opts.url_id;
            ds[ opts.url_base ] || ( ds[ opts.url_base ] = {} );
            var org = ds[ opts.url_base ][ id ] || {};
            ds[ opts.url_base ][ id ] = _.extend( org, model.toJSON(), { id: id } );
            cb( null, _( ds[ opts.url_base ][ id ] ).clone() );

        }).read( function( model, opts, cb ) {
            var id = opts.url_id;
            if ( !ds[ opts.url_base ] || !ds[ opts.url_base ][ id ] ) {
                var err = new Error( "Object ID: " + opts.url_id + " not found" );
                return cb( err );
            }
            cb( null, _( ds[ opts.url_base ][ id ] ).clone() );

        }).delete( function( model, opts, cb ) {
            var id = opts.url_id;
            if ( !ds[ opts.url_base ] || !ds[ opts.url_base ][ id ] ) {
                var err = new Error( "Object ID: " + opts.url_id + " not found" );
                return cb( err );
            }
            var old = ds[ opts.url_base ][ id ];
            delete ds[ opts.url_base ][ id ];
            cb( null, old );

        }).search( function( collection, opts, cb ) {
            var models = _( ds[ opts.url_base ] ).values() || [],
                data = opts.data || {},
                sort = data[ "$sort" ] || null,
                skip = data[ "$skip" ] || 0,
                limit = data[ "$limit" ] || models.length;

            delete data[ "$sort" ];
            delete data[ "$skip" ];
            delete data[ "$limit" ];
            models = sift( data, models );

            // sort
            if ( sort ) {
                models = _( models ).sortBy( sort );
            }

            models = models.slice( skip, skip + limit )
            cb( null, models )
        });
};
