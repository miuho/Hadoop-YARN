import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.Set;
import java.util.HashSet;

import tetrisched.*;

public class Client
{
    public static void main(String [] args)
    {
        job_t jobType = job_t.findByValue(5);
        try {
            int alschedport = 9091;
            TTransport transport = new TSocket("localhost", alschedport);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            TetrischedService.Client client = new TetrischedService.Client(protocol);

            // Do stuff here
            // method: AddJob(int jobId, job_t jobType, int k, int priority, double duration, double slowDuration)
            client.AddJob(7, jobType, 3, 6, 1.0, 3.0);
            Set<Integer> machines = new HashSet<Integer>();
            machines.add(2);
            machines.add(9);
            machines.add(4);
            client.FreeResources(machines);

            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        } 
    }
}
