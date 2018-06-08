import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import tetrisched.*;

public class Server
{
    private void start()
    {
        try {
            int yarnport = 9090;
            YARNTetrischedHandler handler = new YARNTetrischedHandler();
            YARNTetrischedService.Processor processor = new YARNTetrischedService.Processor(handler);

            TServerTransport serverTransport = new TServerSocket(yarnport);
            TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[])
    {
        Server server = new Server();
        server.start();
    }
}
