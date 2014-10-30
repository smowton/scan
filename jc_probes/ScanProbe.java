
import java.util.HashMap;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.IOException;
import org.json.JSONTokener;
import org.json.JSONObject;
import org.json.JSONArray;

import eu.celarcloud.jcatascopia.probepack.Probe;
import eu.celarcloud.jcatascopia.probepack.ProbeMetric;
import eu.celarcloud.jcatascopia.probepack.ProbePropertyType;


public class ScanProbe extends Probe{
	
	private static int DEFAULT_SAMPLING_PERIOD = 10;
	private static String DEFAULT_PROBE_NAME = "ScanProbe";
	private static String SCAN_HOST = "localhost";
	private static long SCAN_PORT = 8080;

	private List<String> classes;

	public ScanProbe(String name, int freq) {
		super(name, freq);

		try {

			InputStream is = getStream("getclasses");
			JSONTokener tok = new JSONTokener(is);
			JSONArray jsclasses = new JSONArray(tok);

			classes = new ArrayList<String>();

			for(int i = 0, ilim = jsclasses.length(); i != ilim; ++i)
				classes.add(jsclasses.getString(i));

			is.close();

		}
		catch(Exception e) {

			System.err.println("Exception " + e.toString() + " creating SCAN probe");
			e.printStackTrace(System.err);
			return;

		}
			
		int idx = 0;
			
		for(String c : classes) {

			this.addProbeProperty(idx + 0, c + "_queueLength",ProbePropertyType.LONG,"", c + " queue length");
			this.addProbeProperty(idx + 1, c + "_workPerHour",ProbePropertyType.DOUBLE,"", c + " work units per hour");
			this.addProbeProperty(idx + 2, c + "_avgCpuUsage", ProbePropertyType.DOUBLE, "", c + " average task CPU usage");
			this.addProbeProperty(idx + 3, c + "_avgMemoryUsage", ProbePropertyType.DOUBLE, "", c + " average task memory usage");
			this.addProbeProperty(idx + 4, c + "_workerUtilisation", ProbePropertyType.DOUBLE, "", c + " worker pool utilisation (1 = all active, 0 = all idle)");

			idx += 5;

		}
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

	private long getQueueLength(String c) throws MalformedURLException, IOException {

		InputStream is = getStream("lsprocs?classname=" + c);
		JSONTokener tok = new JSONTokener(is);
		JSONObject a = new JSONObject(tok);
		long ret = 0;

		for(Iterator<String> keys = a.keys(); keys.hasNext();) {

		    JSONObject val = a.getJSONObject(keys.next());
		    if(val.isNull("worker"))
			++ret;

		}

		is.close();
		return ret;

	}

	private double getWorkerUtilisation(String c) throws MalformedURLException, IOException {

		InputStream is = getStream("lsworkers?classname=" + c);
		JSONTokener tok = new JSONTokener(is);
		JSONObject a = new JSONObject(tok);
		long totalWorkers = a.length();
		long busyWorkers = 0;

		for(Iterator<String> keys = a.keys(); keys.hasNext();) {

			JSONObject val = a.getJSONObject(keys.next());
			if(val.getBoolean("busy"))
				++busyWorkers;

		}

		if(totalWorkers == 0)
			return 0;
		else
			return ((double)busyWorkers) / totalWorkers;

	}

	private double getWorkPerHour(String c) throws MalformedURLException, IOException {

		String wph = getString("getwph?classname=" + c);
		return Double.parseDouble(wph);		

	}

	private void getResourceUsage(HashMap<Integer, Object> values, String c, int offset) throws MalformedURLException, IOException {

//		InputStream is = getStream("getresusage?classname=" + c);
//		JSONTokener tok = new JSONTokener(is);
//		JSONObject stats = new JSONObject(tok);
//		values.put(offset + 2, stats.getDouble("cpu"));
//		values.put(offset + 3, stats.getDouble("mem"));
//		is.close();
		values.put(offset + 2, 0.0);
		values.put(offset + 3, 0.0);

	}

	public ProbeMetric collectOrThrow() throws Exception {

		HashMap<Integer,Object> values = new HashMap<Integer,Object>();
		
		int offset = 0;

		for(String c : classes) {
		
			long qlen = getQueueLength(c);
			double ut = getWorkerUtilisation(c);
			double wph = getWorkPerHour(c);
		
			values.put(offset + 0, qlen);
			values.put(offset + 1, wph);
			values.put(offset + 4, ut);

			System.out.printf("Qlen: %d, wph: %g, utilisation: %g\n", qlen, wph, ut);

			getResourceUsage(values, c, offset);

			offset += 5;

		}
				
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
