import org.apache.thrift.TException;
import java.util.Set;

import tetrisched.*;

public class YARNTetrischedHandler implements YARNTetrischedService.Iface
{
    public void AllocResources(int jobId, Set<Integer> machines) throws org.apache.thrift.TException
    {
        System.out.println("JobId: " + jobId);
        for (Integer m : machines) {
            System.out.println(m);
        }
    }
}
