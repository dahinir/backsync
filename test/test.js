var assert = require( "assert" );

var backbone = require( "backbone" );
var mongodb = require( "mongodb" );
var _ = require( "underscore" );
var async = require( "async" );

var backsync = require( ".." );

var modelsToObject = function( models ) {
    return models.reduce(function( memo, model ) {
        var obj = {}
        obj[ model.id ] = model.toJSON();
        _.extend( memo, obj )
        return memo
    }, {});
}

describe( "backsync", function() {

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


});

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
    });


    it( "creates a new model", function( done ) {
        new Model().on( "sync", function() {
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


    it( "throws error on missing model", function( done ) {
        new Model({ id: this.id })
            .once( "error", function( m, err ) {
                assert( err instanceof backsync.NotFoundError );
                done();
            }).fetch();
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
        var parallels = [ 8, 20, 2 ].map( function( age ) {
            return function( cb ) {
                new Model({ age: age }).on( "sync", function() {
                    cb( null, this );
                }).save();
            }
        })
        async.parallel( parallels, function( err, models ) {
            models = modelsToObject( models )
            new Collection()
                .once( "sync", function() {
                    assert.deepEqual( modelsToObject( this.models ), models );
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
    });

});


describe( "backbone.mongodb", function() {

    var collection = null;
    before(function( done ) {
        var dsn = "mongodb://127.0.0.1:27017/test_backsyncx";
        mongodb.MongoClient.connect( dsn, function( err, db ) {
            collection = db.collection( "models" );
            done();
        } )
    })

    beforeEach(function( done ) {
        collection.drop( function( err ) {
            if ( err && err.toString() != "MongoError: ns not found" ) throw err;
            done();
        });
    })

    var Collection = backbone.Collection.extend({
        url: "mongodb://127.0.0.1:27017/test_backsyncx/models",
        sync: backsync.mongodb()
    });

    var Model = backbone.Model.extend({
        urlRoot: Collection.prototype.url,
        sync: backsync.mongodb()
    });


    it( "creates a new model", function( done ) {
        new Model().on( "sync", function() {
            var id = this.id;
            assert( id ); // id was automaticaly created
            assert.equal( this.get( "hello" ), "world" );
            collection.find({}).toArray(function( err, docs ) {
                assert.equal( docs.length, 1 );
                assert.equal( docs[ 0 ].hello, "world" );
                assert.deepEqual( docs[ 0 ]._id, id );
                done();
            });
        } ).save({ hello: "world" });
    });


    it( "updates an existing model", function( done ) {
        new Model({ id: "cookie" })
            .once( "sync", function() {
                assert.equal( this.id, "cookie" );
                assert.equal( this.get( "foo" ), "bar" );

                // now override
                new Model({ id: "cookie" })
                    .once( "sync", function() {
                        assert.equal( this.id, "cookie" );
                        assert( !this.has( "foo" ) );
                        assert.equal( this.get( "hello" ), "world" );

                        collection.find({}).toArray(function( err, docs ) {
                            assert.equal( docs.length, 1 );
                            assert.equal( docs[ 0 ].hello, "world" );
                            assert.equal( docs[ 0 ]._id, "cookie" );
                            assert.equal( typeof docs[ 0 ].foo, "undefined" );
                            done();
                        });
                    }).save({ "hello": "world" });
            }).save({ foo: "bar" });
    });


    it( "patches an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                assert.equal( this.get( "foo" ), "bar" );

                // now extend
                new Model({ id: this.id.toString() })
                    .once( "sync", function() {
                        assert( this.has( "foo" ) );
                        assert.equal( this.get( "hello" ), "world" );
                        assert.equal( this.get( "foo" ), "bar" );

                        collection.find({}).toArray(function( err, docs ) {
                            assert.equal( docs.length, 1 );
                            assert.equal( docs[ 0 ].hello, "world" );
                            assert.equal( docs[ 0 ].foo, "bar" );
                            done();
                        });
                    }).save({ "hello": "world" }, { patch: true });
            }).save({ foo: "bar" });
    });


    it( "reads an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                new Model({ id: this.id.toString() })
                    .once( "sync", function() {
                        assert.equal( this.get( "hello" ), "world" );
                        done();
                    }).fetch();
            }).save({ hello: "world" });
    });


    it( "fails to read a non-existing model", function( done ) {
        new Model({ id: this.id })
            .once( "error", function( m, err ) {
                assert.equal( err.name, "NotFoundError" );
                done();
            })
            .once( "sync", function() {
                assert.fail( "loaded", "NotFoundError" );
            }).fetch();
    });


    it( "deletes an existing model", function( done ) {
        new Model()
            .once( "sync", function() {
                new Model({ id: this.id.toString() })
                    .once( "sync", function() {
                        collection.find({}).toArray(function( err, docs ) {
                            assert.equal( docs.length, 0 );
                            done();
                        });
                    }).destroy();
            }).save({ hello: "world" });
    });


    it( "fails to delete a non-existing model", function( done ) {
        new Model({ id: "123" })
            .once( "error", function( m, err ) {
                assert.equal( err.name, "NotFoundError" );
                done();
            })
            .once( "sync", function() {
                assert.fail( "destroyed", "NotFoundError" );
            }).destroy();
    });


    it( "reads a collection of models", function( done ) {
        var parallels = [ 8, 20, 2 ].map( function( age ) {
            return function( cb ) {
                new Model({ age: age }).on( "sync", function() {
                    cb( null, this );
                }).save();
            }
        })
        async.parallel( parallels, function( err, models ) {
            models = modelsToObject( models )
            new Collection()
                .once( "sync", function() {
                    assert.equal( this.models.length, 3 );
                    assert.deepEqual( modelsToObject( this.models ), models );
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