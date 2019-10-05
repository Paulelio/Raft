import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Random;

public class SimpleClient {

	public static void main(String[] args) {
		ISimpleClient simple;
		
		try {
			
			simple = (ISimpleClient) Naming.lookup("rmi://localhost/server");
			
			String request = stringGenerator();
			int id = 0;
			String reply = simple.request(request, id);
			
		}catch (RemoteException e) {
			
			System.err.print(e.getMessage());
			
		}catch(MalformedURLException e) {
			
			System.err.print(e.getMessage());
			
		}catch( NotBoundException e) {
			
			System.err.print(e.getMessage());
			
		}
	}
	
	
	private static String stringGenerator() {
		
		byte[] array = new byte[7]; 
	    new Random().nextBytes(array);
	    String generatedString = new String(array, Charset.forName("UTF-8"));
	 
	    return generatedString;
		
	}
}
