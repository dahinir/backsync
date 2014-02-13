var assert = require( "assert" );

var async = require( "async" );
var backsync = require( ".." );
var backbone = require( "backbone" );

describe( "backsync.memory", function() {

    var datastore = {};
    var Model = backbone.Model.extend({
        urlRoot: "/somethings",
        sync: backsync.memory( datastore )
    });

    var Collection = backbone.Collection.extend({
        url: "/somethings",
        sync: backsync.memory( datastore )
    });

    beforeEach(function() {
        for ( var i in datastore ) {
            delete datastore[ i ];
        }
    })

    it( "throws an error if the backsync method isn't implemented", function() {
        var Model = backbone.Model.extend({
            urlRoot: "/fairy",
            sync: backsync()
        });

        assert.throws( function() {
            new Model().save();
        });
    });


    it( "throws an error if no url is specified", function() {
        var Model = backbone.Model.extend({
            url: null,
            sync: backsync()
        });

        assert.throws( function() {
            new Model().save();
        });
    });


    it( "creates a new model", function( done ) {
        var m = new Model().on( "sync", function() {
            assert( this.id ); // id was automaticaly created
            var data = datastore[ "/somethings" ][ this.id ];
            assert.deepEqual( this.attributes, data );
            assert.equal( this.get( "hello" ), "world" );
            done()
        } ).save({ hello: "world" });
    });


    it( "updates an existing model", function( done ) {
        new Model({ id: "cookie" })
            .once( "sync", function() {
                assert.equal( this.id, "cookie" );
                var data = datastore[ "/somethings" ][ this.id ];
                assert.deepEqual( this.attributes, data );
                assert.equal( this.get( "foo" ), "bar" );

                // now override
                new Model({ id: "cookie" })
                    .once( "sync", function() {
                        assert.equal( this.id, "cookie" );
                        var data = datastore[ "/somethings" ][ this.id ];
                        assert.deepEqual( this.attributes, data );
                        assert( !this.has( "foo" ) );
                        assert.equal( this.get( "hello" ), "world" );
                        done();
                    }).save({ "hello": "world" });
            }).save({ foo: "bar" });
    });


    it( "patches an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                var data = datastore[ "/somethings" ][ this.id ];
                assert.deepEqual( this.attributes, data );
                assert.equal( this.get( "foo" ), "bar" );

                // now extend
                new Model({ id: this.id })
                    .once( "sync", function() {
                        var data = datastore[ "/somethings" ][ this.id ];
                        assert.deepEqual( this.attributes, data );
                        assert( this.has( "foo" ) );
                        assert.equal( this.get( "hello" ), "world" );
                        assert.equal( this.get( "foo" ), "bar" );
                        done();
                    }).save({ "hello": "world" }, { patch: true });
            }).save({ foo: "bar" });
    });


    it( "reads an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                new Model({ id: this.id })
                    .once( "sync", function() {
                        var data = datastore[ "/somethings" ][ this.id ];
                        assert.deepEqual( this.attributes, data );
                        assert.equal( this.get( "hello" ), "world" );
                        done();
                    }).fetch();
            }).save({ hello: "world" });
    });


    it( "deletes an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                var id = this.id;
                assert( datastore[ "/somethings" ][ id ] );
                new Model({ id: this.id })
                    .once( "sync", function() {
                        assert( !datastore[ "/somethings" ][ id ] );
                        done();
                    }).destroy();
            }).save({ hello: "world" });
    });


    it( "reads a collection of models", function( done ) {
        async.parallel([
            function( cb ) {
                new Model({ age: 8 }).on( "sync", function() {
                    cb( null, this )
                }).save();
            },
            function( cb ) {
                new Model({ age: 20 }).on( "sync", function() {
                    cb( null, this )
                }).save();
            },
            function( cb ) {
                new Model({ age: 2 }).on( "sync", function() {
                    cb( null, this )
                }).save();
            },
        ], function( err, models ) {
            new Collection()
                .once( "sync", function() {
                    assert.equal( this.models.length, 3 );
                    assert.deepEqual( this.models[0].toJSON(), models[0].toJSON() );
                    assert.deepEqual( this.models[1].toJSON(), models[1].toJSON() );
                    assert.deepEqual( this.models[2].toJSON(), models[2].toJSON() );
                    done();
                }).fetch();
        });
    });


    it( "filters, sorts, skips and limits a collection of models", function( done ) {
        var data = [
            { age: 30, color: "blue" },
            { age: 2, color: "green" },
            { age: 15, color: "blue" },
            { age: 49, color: "blue" },
            { age: 7, color: "blue" }
        ];

        var parallels = data.map( function( entry ) {
            return function( cb ) {
                new Model( entry ).on( "sync", function() {
                    cb( null, this )
                } ).save();
            }
        });

        async.parallel( parallels, function( err, models ) {
            new Collection()
                .once( "sync", function() {
                    var ages = this.models.map(function( model ) {
                        return model.get( "age" );
                    });
                    assert.deepEqual( ages, [ 15, 30 ] );
                    done();
                }).fetch({ data: {
                    color: "blue",
                    $sort: "age",
                    $skip: 1,
                    $limit: 2
                }});
        })
    })

});