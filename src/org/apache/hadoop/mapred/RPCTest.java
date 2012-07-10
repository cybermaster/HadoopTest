
package org.apache.hadoop.mapred;

import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import java.io.IOException;

public class RPCTest extends {

    public static Logger log = Logger.getLogger(RPCTest.class);
    JobConf conf = new JobConf();
    String testjob = "Hadoop RPC test";
    JobTracker trackerObj = null;
    Object obj;
    String bindAddr = "10.1.1.10";
    int port = 8080;
    RPC.Server serverObj = new RPC.Server(obj, conf, bindAddr, port);

//    private static class Invocation implements Writable, Configurable {
//        private String methodName;
//        private Class[] parameterClasses;
//        private Object[] parameters;
//        private Configuration conf;
//
//        public Invocation() {}
//
//        public Invocation(Method method, Object[] parameters) {
//            this.methodName = method.getName();
//            this.parameterClasses = method.getParameterTypes();
//            this.parameters = parameters;
//        }
//
//        /** The name of the method invoked. */
//        public String getMethodName() { return methodName; }
//
//        /** The parameter classes. */
//        public Class[] getParameterClasses() { return parameterClasses; }
//
//        /** The parameter instances. */
//        public Object[] getParameters() { return parameters; }
//
//        public void readFields(DataInput in) throws IOException {
//            methodName = UTF8.readString(in);
//            parameters = new Object[in.readInt()];
//            parameterClasses = new Class[parameters.length];
//            ObjectWritable objectWritable = new ObjectWritable();
//            for (int i = 0; i < parameters.length; i++) {
//                parameters[i] = ObjectWritable.readObject(in, objectWritable, this.conf);
//                parameterClasses[i] = objectWritable.getDeclaredClass();
//            }
//        }
//
//        public void write(DataOutput out) throws IOException {
//            UTF8.writeString(out, methodName);
//            out.writeInt(parameterClasses.length);
//            for (int i = 0; i < parameterClasses.length; i++) {
//                ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
//                        conf);
//            }
//        }
//    }
//    public static class Server extends org.apache.hadoop.ipc.Server {
//        private Object instance;
//        private boolean verbose;
//
//        /** Construct an RPC server.
//         * @param instance the instance whose methods will be called
//         * @param conf the configuration to use
//         * @param bindAddress the address to bind on to listen for connection
//         * @param port the port to listen for connections on
//         */
//        public Server(Object instance, Configuration conf, String bindAddress, int port)
//                throws IOException {
//            this(instance, conf,  bindAddress, port, 1, false, null);
//        }
//
//        public Server(Object instance, Configuration conf, String bindAddress,  int port,
//                      int numHandlers, boolean verbose,
//                      SecretManager<? extends TokenIdentifier> secretManager)
//                throws IOException {
//            super(bindAddress, port, Invocation.class, numHandlers, conf,
//                    classNameBase(instance.getClass().getName()), secretManager);
//            this.instance = instance;
//            this.verbose = verbose;
//        }
//        public Writable call(Class<?> protocol, Writable param, long receivedTime)
//                throws IOException {
//            try {
//                Invocation call = (Invocation)param;
//                if (verbose) log("Call: " + call);
//
//                Method method =
//                        protocol.getMethod(call.getMethodName(),
//                                call.getParameterClasses());
//                method.setAccessible(true);
//
//                long startTime = System.currentTimeMillis();
//                Object value = method.invoke(instance, call.getParameters());
//                int processingTime = (int) (System.currentTimeMillis() - startTime);
//                int qTime = (int) (startTime-receivedTime);
//                if (LOG.isDebugEnabled()) {
//                    LOG.debug("Served: " + call.getMethodName() +
//                            " queueTime= " + qTime +
//                            " procesingTime= " + processingTime);
//                }
//                rpcMetrics.addRpcQueueTime(qTime);
//                rpcMetrics.addRpcProcessingTime(processingTime);
//                rpcMetrics.addRpcProcessingTime(call.getMethodName(), processingTime);
//                if (verbose) log("Return: "+value);
//
//                return new ObjectWritable(method.getReturnType(), value);
//
//            } catch (InvocationTargetException e) {
//                Throwable target = e.getTargetException();
//                if (target instanceof IOException) {
//                    throw (IOException)target;
//                } else {
//                    IOException ioe = new IOException(target.toString());
//                    ioe.setStackTrace(target.getStackTrace());
//                    throw ioe;
//                }
//            } catch (Throwable e) {
//                if (!(e instanceof IOException)) {
//                    LOG.error("Unexpected throwable object ", e);
//                }
//                IOException ioe = new IOException(e.toString());
//                ioe.setStackTrace(e.getStackTrace());
//                throw ioe;
//            }
//        }
//    }

    public RPCTest() throws IOException, InterruptedException {
        trackerObj = new JobTracker(conf, testjob);
        log.debug("this is a sample log message.");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        log.debug("this is a sample log message in main().");
        RPCTest testObj = new RPCTest();

    }



}