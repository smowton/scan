
import java.util.HashMap;
import java.util.Random;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.IOException;
import org.json.JSONTokener;
import org.json.JSONObject;

import eu.celarcloud.jcatascopia.probepack.Probe;
import eu.celarcloud.jcatascopia.probepack.ProbeMetric;
import eu.celarcloud.jcatascopia.probepack.ProbePropertyType;


public class ScanProbe extends Probe{
	
	private static int DEFAULT_SAMPLING_PERIOD = 10;
	private static String DEFAULT_PROBE_NAME = "ScanProbe";
	private static String SCAN_HOST = "snf-538017.vm.okeanos.grnet.gr";
	private static long SCAN_PORT = 8080;

	public ScanProbe(String name, int freq) {
		super(name, freq);
		this.addProbeProperty(0, "queueLength",ProbePropertyType.LONG,"","Queue length");
		this.addProbeProperty(1,"workPerHour",ProbePropertyType.DOUBLE,"","Work units per hour");
		this.addProbeProperty(2, "avgCpuUsage", ProbePropertyType.DOUBLE, "", "Average task CPU usage");
		this.addProbeProperty(3, "avgMemoryUsage", ProbePropertyType.DOUBLE, "", "Average task memory usage");
	}
	
	public ScanProbe(){
		this(DEFAULT_PROBE_NAME, DEFAULT_SAMPLING_PERIOD);
	}

	private InputStream getStream(String relurl) throws MalformedURLException, IOException {

		String url = String.format("http://%s:%d/%s", SCAN_HOST, SCAN_PORT, relurl);
		return new URL(url).openStream();

	}

	private String getString(String relurl) throws MalformedURLException, IOException {

		InputStream is = getStream(relurl);
		InputStreamReader isr = new InputStreamReader(is);
		StringWriter sw = new StringWriter();

		char[] buf = new char[4096];
		
		int lastRead;

		while((lastRead = isr.read(buf, 0, 4096)) != -1)
			sw.write(buf, 0, lastRead);

		isr.close();
		return sw.toString();

	}

	private long getQueueLength() throws MalformedURLException, IOException {

		InputStream is = getStream("lsprocs?classname=linux");
		JSONTokener tok = new JSONTokener(is);
		JSONObject a = new JSONObject(tok);
		long ret = a.length();
		is.close();
		return ret;

	}

	private double getWorkPerHour() throws MalformedURLException, IOException {

		String wph = getString("getwph?classname=linux");
		return Double.parseDouble(wph);		

	}

	private void getResourceUsage(HashMap<Integer, Object> values) throws MalformedURLException, IOException {

		InputStream is = getStream("getresusage?classname=linux");
		JSONTokener tok = new JSONTokener(is);
		JSONObject stats = new JSONObject(tok);
		values.put(2, stats.get("cpu"));
		values.put(3, stats.get("mem"));

	}

	public ProbeMetric collectOrThrow() throws Exception {

		HashMap<Integer,Object> values = new HashMap<Integer,Object>();
		
		long qlen = getQueueLength();
		double wph = getWorkPerHour();
		
		values.put(0, qlen);
		values.put(1, wph);

		getResourceUsage(values);
				
		return new ProbeMetric(values);

	}
    
	@Override
	public ProbeMetric collect() {

		try {

			return collectOrThrow();

		}
		catch(Exception e) {

			System.err.printf("Exception collection SCAN probe data: %s\n", e.toString());
			return new ProbeMetric(new HashMap<Integer, Object>());

		}

	}

	@Override
	public String getDescription() {
		return "Queue length and work units/h for SCAN tasks";
	}
	
	public static void main(String[] args) {
		ScanProbe p = new ScanProbe();
		p.activate();
	}
}
