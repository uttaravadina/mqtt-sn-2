/*
 *
 *  * Copyright (c) 2021 Simon Johnson <simon622 AT gmail DOT com>
 *  *
 *  * Find me on GitHub:
 *  * https://github.com/simon622
 *  *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package org.slj.mqtt.sn.net;

import org.slj.mqtt.sn.impl.AbstractMqttsnTransport;
import org.slj.mqtt.sn.model.INetworkContext;
import org.slj.mqtt.sn.spi.IMqttsnMessage;
import org.slj.mqtt.sn.spi.IMqttsnRuntimeRegistry;
import org.slj.mqtt.sn.spi.MqttsnException;
import org.slj.mqtt.sn.spi.NetworkRegistryException;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * TCP IP implementation support. Can be run with SSL (TLS) enabled for secure communication.
 * Supports running in both client and server mode.
 */
public class MqttsnTcpTransport
        extends AbstractMqttsnTransport<IMqttsnRuntimeRegistry> {

    protected final MqttsnTcpOptions options;
    protected boolean clientMode = false;

    static AtomicInteger connectionCount = new AtomicInteger(0);

    protected Handler clientHandler;
    protected Server server;

    public MqttsnTcpTransport(MqttsnTcpOptions options, boolean clientMode) {
        this.options = options;
        this.clientMode = clientMode;
    }

    @Override
    public void start(IMqttsnRuntimeRegistry runtime) throws MqttsnException {
        try {
            super.start(runtime);
            if(options.isSecure()){
            }
            if(clientMode){
                logger.log(Level.INFO, String.format("running in client mode, establishing tcp connection..."));
                connectClient();
            } else {
                logger.log(Level.INFO, String.format("running in server mode, establishing tcp acceptors..."));
                startServer();
            }
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }

    @Override
    public void stop() throws MqttsnException {
        super.stop();
        try {
            if(clientMode){
                logger.log(Level.INFO, String.format("closing in client mode, closing tcp connection(s)..."));
                if(clientHandler != null){
                    clientHandler.close();
                }
            } else {
                logger.log(Level.INFO, String.format("closing in server mode, closing tcp connection(s)..."));
                if(server != null){
                    server.close();
                }
            }
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }

    @Override
    protected void writeToTransport(INetworkContext context, ByteBuffer data) throws MqttsnException {
        try {
            if(clientMode){
                clientHandler.write(drain(data));
            } else {
                server.write(context, drain(data));
            }
        } catch(IOException e){
            throw new MqttsnException("error writing to connection;", e);
        }
    }

    protected SSLContext initSSLContext()
            throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException, UnrecoverableKeyException, KeyManagementException {

        String keyStorePath = options.getKeyStorePath();
        String trustStorePath = options.getTrustStorePath();
        logger.log(Level.INFO, String.format("initting SSL context with keystore [%s], truststore [%s]", keyStorePath, trustStorePath));

        //-- keystore
        KeyStore keyStore = null;
        KeyManager[] keyManagers = null;
        if(keyStorePath != null){
            File f = new File(keyStorePath);
            if(!f.exists() || !f.canRead())
                throw new KeyStoreException("unable to read keyStore " + keyStorePath);

            keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            KeyManagerFactory kmf = KeyManagerFactory
                    .getInstance(KeyManagerFactory.getDefaultAlgorithm());

            try(InputStream keyStoreData = new FileInputStream(f)) {
                char[] password = options.getKeyStorePassword().toCharArray();
                keyStore.load(keyStoreData, password);
                Enumeration<String> aliases = keyStore.aliases();
                while(aliases.hasMoreElements()){
                    String el = aliases.nextElement();
                    logger.log(Level.INFO, String.format("keystore contains alias [%s]", el));
                }
                kmf.init(keyStore, password);
                keyManagers = kmf.getKeyManagers();
            }
        }

        TrustManager[] trustManagers = null;
        if(trustStorePath == null){
            logger.log(Level.WARNING, "!! ssl operating in trust-all mode, do not use this in production, nominate a trust store !!");
            TrustManager trustAllCerts = new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
                public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                }
                public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                }
            };
            trustManagers = new TrustManager[] { trustAllCerts };
        } else {
            File f = new File(trustStorePath);
            if(!f.exists() || !f.canRead())
                throw new KeyStoreException("unable to read trustStore " + trustStorePath);

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try(InputStream trustStoreData = new FileInputStream(f)) {
                char[] password = options.getKeyStorePassword().toCharArray();
                trustStore.load(trustStoreData, password);
                Enumeration<String> aliases = keyStore.aliases();
                while(aliases.hasMoreElements()){
                    String el = aliases.nextElement();
                    logger.log(Level.INFO, String.format("truststore contains alias [%s]", el));
                }
                TrustManagerFactory tmf = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(keyStore);
                trustManagers = tmf.getTrustManagers();
            }
        }
        SSLContext ctx = SSLContext.getInstance(options.getSslAlgorithm());
        logger.log(Level.INFO, String.format("ssl initialised with algo [%s]", ctx.getProtocol()));
        logger.log(Level.INFO, String.format("ssl initialised with JCSE provider [%s]", ctx.getProvider()));
        logger.log(Level.INFO, String.format("ssl initialised with [%s] key manager(s)", keyManagers == null ? null : keyManagers.length));
        logger.log(Level.INFO, String.format("ssl initialised with [%s] trust manager(s)", trustManagers == null ? null : trustManagers.length));
        ctx.init(keyManagers, trustManagers,
                SecureRandom.getInstanceStrong());
        return ctx;

    }

    protected void connectClient() throws MqttsnException {
        try {
            //-- check we have a valid network location to connect to
            Optional<INetworkContext> remoteAddress = registry.getNetworkRegistry().first();
            if(!remoteAddress.isPresent()) throw new MqttsnException("need a remote location to connect to found <null>");
            INetworkContext remoteContext = remoteAddress.get();

            //-- create and bind local socket
            if(clientHandler == null){
                Socket clientSocket;
                if(options.isSecure()){
                    clientSocket = initSSLContext().getSocketFactory().createSocket();
                    SSLSocket ssl = (SSLSocket) clientSocket;
                    if(options.getSslProtocols() != null) {
                        logger.log(Level.INFO, String.format("starting client-ssl with protocols [%s]",
                                Arrays.toString(options.getSslProtocols())));
                        ssl.setEnabledProtocols(options.getSslProtocols());
                    }
                    if(options.getCipherSuites() != null){
                        logger.log(Level.INFO, String.format("starting client-ssl with cipher suites [%s]",
                                Arrays.toString(options.getCipherSuites())));
                        ssl.setEnabledCipherSuites(options.getCipherSuites());
                    }

                    logger.log(Level.INFO, String.format("client-ssl enabled protocols [%s]",
                            Arrays.toString(ssl.getEnabledProtocols())));
                    logger.log(Level.INFO, String.format("client-ssl enabled cipher suites [%s]",
                            Arrays.toString(ssl.getEnabledCipherSuites())));

                } else {
                    clientSocket = options.getClientSocketFactory().createSocket();
                }

                //bind to the LOCAL address if specified, else bind to the OS default
                InetSocketAddress localAddress = null;
                if(options.getHost() != null){
                    localAddress = new InetSocketAddress(options.getHost(), options.getPort());
                }

                clientSocket.bind(localAddress);
                clientSocket.setSoTimeout(options.getSoTimeout());
                clientSocket.setKeepAlive(options.isTcpKeepAliveEnabled());

                //-- if bound locally, connect to the remote if specified (running as a client)
                if(clientSocket.isBound()){
                    InetSocketAddress r =
                            new InetSocketAddress(remoteContext.getNetworkAddress().getHostAddress(),
                                    remoteContext.getNetworkAddress().getPort());
                    logger.log(Level.INFO, String.format("connecting client to [%s] -> [%s]",
                            remoteContext.getNetworkAddress().getHostAddress(), remoteContext.getNetworkAddress().getPort()));
                    clientSocket.connect(r, options.getConnectTimeout());
                }

                //-- need to be connected before handshake
                if(clientSocket instanceof SSLSocket &&
                        options.isSecure()){
                    ((SSLSocket) clientSocket).startHandshake();
                }

                if(clientSocket.isBound() && clientSocket.isConnected()){
                    clientHandler = new Handler(remoteContext, clientSocket, null);
                    clientHandler.start();
                } else {
                    logger.log(Level.SEVERE, "could not bind and connect socket to host, finished.");
                }
            }
        } catch(Exception e){
            throw new MqttsnException(e);
        }
    }

    protected void startServer()
            throws IOException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, UnrecoverableKeyException {
        if(server == null && running){
            ServerSocket socket = null;
            if(options.isSecure()){
                logger.log(Level.INFO, String.format("running in secure mode, tcp with TLS..."));
                socket = initSSLContext().getServerSocketFactory().createServerSocket(options.getSecurePort());

                SSLServerSocket ssl = (SSLServerSocket) socket;
                if(options.getSslProtocols() != null) {
                    logger.log(Level.INFO, String.format("starting server-ssl with protocols [%s]", Arrays.toString(options.getSslProtocols())));
                    ssl.setEnabledProtocols(options.getSslProtocols());
                }
                if(options.getCipherSuites() != null){
                    logger.log(Level.INFO, String.format("starting server-ssl with cipher suites [%s]", Arrays.toString(options.getCipherSuites())));
                    ssl.setEnabledCipherSuites(options.getCipherSuites());
                }

                logger.log(Level.INFO, String.format("server-ssl enabled protocols [%s]", Arrays.toString(ssl.getEnabledProtocols())));
                logger.log(Level.INFO, String.format("server-ssl enabled cipher suites [%s]", Arrays.toString(ssl.getEnabledCipherSuites())));

            } else {
                socket = options.getServerSocketFactory().createServerSocket(options.getPort());
            }
            server = new Server(socket);
            server.start();
        }
    }

    private class Server extends Thread implements ClosedListener{

        private final ServerSocket serverSocket;
        private Map<INetworkContext, Handler> connections =
                Collections.synchronizedMap(new HashMap<>());

        public Server(ServerSocket serverSocket){
            this.serverSocket = serverSocket;
            setPriority(Thread.MIN_PRIORITY + 1);
            setDaemon(false);
            setName("mqtt-sn-tcp-server");
        }

        public void write(INetworkContext context, byte[] data) throws IOException {
            Handler handler = connections.get(context);
            if(handler == null) throw new IOException("no connected handler for context");
            handler.write(data);
        }

        public void close(){
            if(connections != null && !connections.isEmpty()){
                connections.values().stream().forEach(c -> {
                    try {
                        c.close();
                    } catch(Exception e){
                        logger.log(Level.SEVERE, "error closing connection", e);
                    }
                });
            }
        }

        public void closed(Handler handler){
            if(connections != null && handler != null)
                connections.remove(handler.descriptor.context);
        }

        public void run(){
            logger.log(Level.INFO, String.format("starting TCP listener, accepting [%s] connections on port [%s]",
                    options.getMaxClientConnections(), serverSocket.getLocalPort()));
            while(running){
                try {
                    Socket socket = serverSocket.accept();
                    if(connections.size() >= options.getMaxClientConnections()){
                        logger.log(Level.WARNING, "max connection limit reached, disconnecting socket");
                        socket.close();

                    } else {
                        SocketAddress address = socket.getRemoteSocketAddress();
                        logger.log(Level.INFO,
                                String.format("new connection accepted from [%s]", address));

                        NetworkAddress networkAddress = NetworkAddress.from((InetSocketAddress) address);
                        INetworkContext context = registry.getNetworkRegistry().getContext(networkAddress);
                        if(context == null){
                            //-- if the network context does not exist in the registry, a new one is created by the factory -
                            //- NB: this is NOT auth, this is simply creating a context to which we can respond, auth can
                            //-- happen during the mqtt-sn context creation, at which point we can talk back to the device
                            //-- with error packets and the like
                            context = registry.getContextFactory().createInitialNetworkContext(networkAddress);
                        }
                        Handler handler = new Handler(context, socket, this);
                        connections.put(context, handler);
                        handler.start();
                    }

                } catch (NetworkRegistryException | IOException | MqttsnException e){
                    logger.log(Level.SEVERE, "error encountered accepting connection;", e);
                }
            }
        }
    }

    /**
     * When running in server mode the handler will accept each connection lazily (not pre-pooled)
     */
    class Handler extends Thread {

        private final SocketDescriptor descriptor;
        private final ClosedListener listener;

        public Handler(INetworkContext context, Socket socket, ClosedListener listener) throws IOException {
            this.descriptor = new SocketDescriptor(context, socket);
            this.listener = listener;
            setName("mqtt-sn-tcp-connection-" + connectionCount.incrementAndGet());
            setDaemon(false);
            setPriority(Thread.NORM_PRIORITY);
        }

        public void close() throws IOException {
            logger.log(Level.INFO, String.format("handler is closing [%s]", descriptor));
            descriptor.close();
            if(listener != null) listener.closed(this);
        }

        public void write(byte[] data) throws IOException {
            if(descriptor.isOpen()){
                logger.log(Level.INFO, String.format("writing [%s] bytes to output stream", data.length));
                descriptor.os.write(data);
                descriptor.os.flush();
            }
        }

        public void run(){
            try {
                logger.log(Level.INFO, String.format("starting socket handler for [%s]", descriptor));
                while(running && descriptor.isOpen()){
                    int count;
                    int messageLength = 0;
                    int messageLengthRemaining = 0;
                    byte[] buff = new byte[options.getReadBufferSize()];
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        while ((count = descriptor.is.read(buff)) > 0) {
                            boolean isNewMessage = baos.size() == 0;
                            baos.write(buff, 0, count);
                            if(isNewMessage){
                                messageLength = registry.getCodec().readMessageSize(baos.toByteArray());
                                messageLengthRemaining = messageLength;
                            }
                            messageLengthRemaining -= count;

                            if (messageLengthRemaining == 0 && baos.size() == messageLength) {
                                logger.log(Level.INFO, String.format("received [%s] bytes from socket for [%s], reset buffer", messageLength, descriptor));
                                receiveFromTransport(descriptor.context, wrap(baos.toByteArray()));
                                messageLength = 0;
                                messageLengthRemaining = 0;
                                baos.reset();
                            }
                        }
                    }

                    if(count == 0 || count == -1) {
                        logger.log(Level.INFO, String.format("received [%s] bytes from socket handler (end of stream), ", count, descriptor));
                        break;
                    }
                }
            }
            catch(SocketException e){
                //-- socket close from underneath during reading
                logger.log(Level.INFO, String.format("socket closed [%s], nothing to read", descriptor));
            }
            catch(IOException e){
                throw new RuntimeException("error accepting data from client stream;",e);
            } finally {
                try {
                    descriptor.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "error closing socket descriptor;", e);
                }
            }
        }
    }

    private class SocketDescriptor implements Closeable {

        private volatile boolean closed = false;
        private INetworkContext context;
        private Socket socket;
        private InputStream is;
        private OutputStream os;

        public SocketDescriptor(INetworkContext context, Socket socket) throws IOException {
            this.context = context;
            this.socket = socket;
            if(socket.isClosed()) throw new IllegalArgumentException("cannot get a descriptor onto closed socket");
            is = socket.getInputStream();
            os = socket.getOutputStream();
        }

        public boolean isOpen(){
            return socket != null && socket.isConnected() && !socket.isClosed();
        }

        @Override
        public String toString() {
            return "SocketDescriptor{" +
                    "context=" + context +
                    '}';
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                if(!closed){
                    connectionCount.decrementAndGet();
                    closed = true;
                    try {is.close();} catch(Exception e){}
                    try {os.close();} catch(Exception e){}
                    try {socket.close();} catch(Exception e){}
                }
            } finally {
                socket = null;
                context = null;
                os = null;
                is = null;
            }
        }
    }

    @Override
    public void broadcast(IMqttsnMessage message) throws MqttsnException {
        throw new UnsupportedOperationException("broadcast not supported on TCP");
    }
}

interface ClosedListener {
    void closed(MqttsnTcpTransport.Handler handler);
}