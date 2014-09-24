/*
 * Copyright (c) the original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.complexible.stardog.ext.groovy

import groovy.util.logging.Log

import com.complexible.common.openrdf.model.Graphs
import com.complexible.stardog.StardogException
import com.complexible.stardog.api.*
import com.complexible.stardog.reasoning.api.ReasoningType

import org.openrdf.model.Resource
import org.openrdf.model.Value
import org.openrdf.model.impl.LiteralImpl
import org.openrdf.model.impl.StatementImpl
import org.openrdf.model.impl.URIImpl
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.query.TupleQueryResult


/**
 * Stardog - Groovy wrapper on top of SNARL for easy access
 * and idiomatic groovy usage of Stardog, such as closure based processing
 *  
 *  Provides simplified abstraction over connection management
 * 
 * @author Al Baker
 * @author Clark & Parsia, LLC
 * @author Conrad Leonard
 *
 */

@Log
class Stardog {

    String to
    String server
    String username
    String password
    int maxPool
    int minPool
    ReasoningType reasoning

    private ConnectionPool pool
    private ConnectionConfiguration connectionConfig
    private ConnectionPoolConfig poolConfig
    
    public Stardog() { }

    public Stardog(Map props) {
        to        = props.to        ?: null
        server    = props.server    ?: "snarl://localhost:5820"
        username  = props.username  ?: null
        password  = props.password  ?: null
        maxPool   = props.maxPool   ?: 100
        minPool   = props.minPool   ?: 10
        reasoning = props.reasoning ?: ReasoningType.NONE

        if (props.home) {
            System.setProperty("stardog.home", props.home)
        }
        
        log.info "Initialized with $props"

        initialize()
    }

    /**
     * Initialize private config members
     */
    void initialize() {

        connectionConfig = ConnectionConfiguration
            .to(to)
            .server(server)
            .credentials(username, password)
            .reasoning(reasoning)

        poolConfig = ConnectionPoolConfig
            .using(connectionConfig)
            .minPool(minPool)
            .maxPool(maxPool)

        pool = poolConfig.create()
    }

    /**
     * Get a connection from the pool.
     * 
     * @return Connection
     */
    public Connection getConnection() {
        try {
            return pool.obtain()
        } catch (StardogException e) {
            log.severe "Error obtaining connection from Stardog pool"
            log.severe e.toString()
            throw new RuntimeException(e)
        }
    }

    /**
     * Release the specified connection to the pool.
     * 
     * @param connection
     */
    public void releaseConnection(Connection connection) {
        try {
            pool.release(connection)
        } catch (StardogException e) {
            log.severe "Error releasing connection from Stardog pool"
            log.severe e.toString()
            throw new RuntimeException(e)
        }
    }

    /**
     * <code>withConnection</code>
     * Execute-around with guaranteed release of connection.
     * 
     * @param Closure to execute over the connection
     */
    public void withConnection(Closure c) {
        Connection con = getConnection()
        try {
            // do stuff
            c(con)
        }
        catch (e) {
            // inform of problems
            log.severe e.toString()
        }
        finally {
            // clean up
            releaseConnection(con)
        }
    }

    /**
     * <code>query</code>
     * @param queryString SPARQL query string
     * @param closure to execute over the result set
     */
    public void query(String queryString, Closure c) {
        query(queryString, null, c)
    }
    
    /**
     * <code>query</code>
     * @param queryString SPARQL query string
     * @param args map of string and object to pass bind as input parameters
     * @param closure to execute over the result set
     */
    public void query(String queryString, Map args, Closure c) {
        
        log.info "queryString: $queryString"
        println queryString
        
        withConnection { con -> 
            
            TupleQueryResult result = null
            SelectQuery query = con.select(queryString)
            
            args?.each {
                query.parameter(it.key, it.value)
            }
            
            result = query.execute()
            while (result.hasNext()) {
                c(result.next())
            }

            result.close()
        }
    }
    
    /**
     * <code>update</code>
     * @param updateString SPARQL update string
     */
    public void update(String queryString) {
        update(queryString, null)
    }
    
    /**
     * <code>update</code>
     * @param updateString SPARQL update string
     * @param args map of string and object to pass bind as input parameters
     */
    public void update(String queryString, Map args) {
        
        log.info "queryString: $queryString"
        println queryString
        
        withConnection { con ->

            def query = con.update(queryString)
            
            args?.each {
                query.parameter(it.key, it.value)
            }
            
            query.execute()
        }
    }
    
    /**
     * <code>each</code>
     * iterates over a Binding result
     * @param queryString
     * @param closure with each SPARQL query bound into the closure
     */
    public void each(String queryString, Closure c) {
        
        log.info "queryString: $queryString"
        println queryString 
        
        withConnection { con ->

            TupleQueryResult result = null
            SelectQuery query = con.select(queryString)
            
            result = query.execute()
            
            while (result.hasNext()) {
                def input = result.next().iterator().collectEntries( {
                    [ (it.getName()) : (it.getValue()) ]
                })
                // binds the Sesame result set as a map into the closure so SPARQL variables
                // become closure native variables, e.g. "x"
                c.delegate = input
                c()
            }

            result.close()
        }
    }

    /**
     * <code>insert</code>
     * Inserts either a single list, or a list of lists of triples
     * assumes URIImpl(s,p)
     * assumes LiteralImpl(o) unless a java.net.URI is passed in, in which case it will insert a URIImpl  
     * @param arr lists
     */
    public void insert(List arr) {
        
        withConnection { con ->
            
            Adder adder = null
            def statements = []
            if (arr.size >= 1) {
                if (arr[0].class == java.util.ArrayList.class) {
                    arr.each { arr2 ->
                        if (arr2.size == 3) {
                            def s = arr2[0]
                            def p = arr2[1]
                            def o = arr2[2]
                            if (o.class == java.net.URI.class) {
                                statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), new URIImpl(o.toString())))
                            }
                            else if (o.class == java.lang.String.class) {
                                statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), new LiteralImpl(o)))
                            }
                            else {
                                statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), o))
                            }
                        }
                    }
                } else {
                    def s = arr[0]
                    def p = arr[1]
                    def o = arr[2]
                    if (o.class == java.net.URI.class) {
                        statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), new URIImpl(o.toString())))
                    }
                    else if (o.class == java.lang.String.class) {
                        statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), new LiteralImpl(o)))
                    }
                    else {
                        statements.add(new StatementImpl(new URIImpl(s), new URIImpl(p), o))
                    }
                }
            }
            
            con.begin()
            con.add().graph(Graphs.newGraph(statements))
            con.commit()
        }
    }

    /**
     * <code>remove</code>
     * @param list of format subject, predicate, object, graph URI
     */
    public void remove(List args) {

        def subject = args[0]
        def predicate = args[1]
        def object = args[2]
        def graphUri = args[3]

        URIImpl subjectResource = null
        URIImpl predicateResource = null
        Resource context = null

        if (subject != null) {
            subjectResource = new URIImpl(subject)
        }
        if (predicate != null) {
            predicateResource = new URIImpl(predicate)
        }

        if (graphUri != null) {
            context = ValueFactoryImpl.getInstance().createURI(graphUri)
        }

        Value objectValue = null
        if (object != null) {
            if (object.class == java.net.URI.class) {
                objectValue = new URIImpl(object.toString())
            }
            else {
                objectValue = TypeConverter.asLiteral(object)
            }
        }

        withConnection { con ->
            con.begin()
            con.remove().statements(subjectResource, predicateResource, objectValue, context)
            con.commit()
        }
    }

}
