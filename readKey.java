package com.lucent.aaa.pluglet;


import com.lucent.aaa.log.Logger;
import com.lucent.aaa.plugin.Java;
import com.lucent.aaa.plugin.Plugin;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.lucent.aaa.log.*;



/**
 * Created with IntelliJ IDEA.
 * User: vaisverm
 * Date: 21/3/18
 * Time: 3:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class ReadKey implements Java.Pluglet{

    static LogAgent s_logAgent = new LogAgent( "ReadKey Logging" );


    /**
     * open is called once and only once to configure the plugin.
     * @param argLine The contents of the Java-Arguments.
     * @param arguments The arguments split on white space and placed in an array.
     * present on the RHS of the map.
     * @throws com.lucent.aaa.plugin.Plugin.OpenException if the plugin fails initialization.
     */
    public void open( String argLine, String[] arguments ) throws Plugin.OpenException
    {


    }

    /**
     * invoke is called once per Java plugin invocation.
     * @param session The session object. Various services are available through it.
     * @param token the continuation token. Suspend/Resume, Challenge, and Continue use this object
     * to provide context, or state.
     * @return a disposition. This disposition will be used by the Java plugin for its own disposition.
     * @throws Plugin.DispositionException a way to return a disposition inside of an exception.
     * @see Java.Session
     */
    public Plugin.Disposition invoke( Java.Session session, Object token ) throws Plugin.DispositionException
    {
        try{
            Record record = null;
            Key readKey = null;
            String hostStr = session.getInputText( "host" );
            String portStr = session.getInputText( "port" );
            String namespaceStr = session.getInputText( "namespace" );
            String setNameStr = session.getInputText( "setName" );
            String ReqKeyIdentityStr = session.getInputText( "reqKeyIdentity" );


            int portInt;
            try
            {
                portInt = Integer.parseInt( portStr );
            }
            catch ( NumberFormatException e )
            {
                return session.makeError( "port not a number" );
            }

            ClientPolicy policy = new ClientPolicy();
            policy.user = session.getInputText("userName");
            policy.password = session.getInputText( "userPassword" );

            Host[] hosts = Host.parseHosts(hostStr, portInt);
            s_logAgent.logAtDebug( "Host is .."+ hostStr );
            s_logAgent.logAtDebug( "Port is .."+portInt  );
            s_logAgent.logAtDebug( "userName is .."+ policy.user  );
            s_logAgent.logAtDebug( "userPassword is .."+ policy.password  );


            s_logAgent.logAtDebug( "Creating Aerospike Client Instance ..");
            AerospikeClient client = new AerospikeClient(policy,hosts);


            s_logAgent.logAtDebug( " Aerospike Client Creation and Connection  is successful .."  );
            //Key to Read the record
            readKey = new Key(namespaceStr,setNameStr,ReqKeyIdentityStr);


            //SELECT keyValue FROM test.myset WHERE PK = 'keyId'
            record = client.get(null, readKey,"keyValue");

            if(record == null){
                throw new Exception(String.format(
                        "Failed to get the keyValue . No such entry exists for keyId =%s",ReqKeyIdentityStr));
            }


            //session.setOutputText( "Records: ", record.toString() );

            //validateKey(ReqKeyIdentityStr,record);
            s_logAgent.logAtInfo( "Aero Records ->" + record.toString() );
            s_logAgent.logAtDebug( "Aero Records ->" + record.toString() );

            Object received = record.getValue("keyValue");
            s_logAgent.logAtInfo( "KeyValue is : ->" + received );
            session.setOutputText( "keyValue", received.toString() );

            client.close();
            return session.makeSuccess( "ReadKey Pluglet Invoked,Key Retrieved:"+ received );
        }
        catch(Exception e){
            s_logAgent.logAtInfo("Inside Catch  Exception");
            String errMsg =    e.getLocalizedMessage();
            session.setOutputText( "Key Retrieval Failed", errMsg );
            //session.makeFailure("Key Retrieval Failed",errMsg) ;
            e.printStackTrace();
            s_logAgent.logAtInfo(e.getLocalizedMessage());
            s_logAgent.logAtInfo(e.getMessage());
        }


        //return session.makeSuccess( "ReadKey Plugin Invoked ");
        return session.makeError( "Errors while retrieving the Privacy Keys" );
    }


    /**
     * close is called once at server shutdown/restart. Non-memory resources should be
     * returned at this time.
     */
    public void close()
    {
        s_logAgent.logAtDebug( "close called" );
    }

    /*private void validateKey(String ReqKeyIdentity, Record record) {
        Object received = record.getValue("keyValue");
        String expected = ReqKeyIdentity;

        if (received != null && received.equals(expected)) {
            s_logAgent.logAtInfo("Received/Expected matched : Expected %s. Received %s" + expected+ received);

        }
        else {
            s_logAgent.logAtInfo("Received/Expected mismatch: Expected %s. Received %s." + expected+ received);
        }
    } */

    }
