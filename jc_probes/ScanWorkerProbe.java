
import java.util.HashMap;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.StringWriter;
import java.io.IOException;
import org.json.JSONTokener;
import org.json.JSONObject;
import org.json.JSONArray;

import eu.celarcloud.jcatascopia.probepack.Probe;
import eu.celarcloud.jcatascopia.probepack.ProbeMetric;
import eu.celarcloud.jcatascopia.probepack.ProbePropertyType;

public class ScanWorkerProbe extends Probe{
	
	private static int DEFAULT_SAMPLING_PERIOD = 10;
	private static String DEFAULT_PROBE_NAME = "ScanWorkerProbe";
	private static long SCAN_PORT = 8080;

        private String scanHost;
	private String scanClass;
	private int workerId;

	public ScanWorkerProbe(String name, int freq) throws Exception {

		super(name, freq);
		this.addProbeProperty(0, "busy", ProbePropertyType.INTEGER, "", "Is this worker running a SCAN job?");

		// TODO remove this hack when there's a good way to find the agent directory
		String agentPath = "";
		try {
		    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/etc/scan_probe")));
		    agentPath = br.readLine().trim();
		}
		catch(Exception e) {
		    System.err.println("No /etc/scan_probe found; using cwd");
		    agentPath = ".";
		}

		Properties p = new Properties();
		FileInputStream fis = null;
		try {
		    fis = new FileInputStream(agentPath + File.separator + "resources" + File.separator + "scanprobe.properties");
		    p.load(fis);
		}
		catch(Exception e) {
		    throw new Exception("Must provide valid properties file 'scanprobe.properties'");
		}
		finally {
		    if(fis != null)
			fis.close();
		}

		scanHost = p.getProperty("host");
		if(scanHost == null)
			scanHost = "localhost";

		scanClass = p.getProperty("class");
		if(scanClass == null)
			throw new Exception("Must specify proprety 'class'");

		try {
			workerId = Integer.parseInt(p.getProperty("workerid"));
		}
		catch(NumberFormatException e) {
			throw new Exception("Must specify integer property workerid");
		}
		catch(NullPointerException e) {
			throw new Exception("Must specify integer property workerid");
		}

	}
	
	public ScanWorkerProbe() throws Exception {
		this(DEFAULT_PROBE_NAME, DEFAULT_SAMPLING_PERIOD);
	}

	private InputStream getStream(String relurl) throws MalformedURLException, IOException {

		String url = String.format("http://%s:%d/%s", scanHost, SCAN_PORT, relurl);
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

	private int getBusy() throws MalformedURLException, IOException {

		InputStream is = getStream("lsworkers?classname=" + scanClass);
		JSONTokener tok = new JSONTokener(is);
		JSONObject a = new JSONObject(tok);
		long ret = 0;

		boolean busy = false;

		try {
			busy = a.getJSONObject(((Integer)workerId).toString()).getBoolean("busy");
			System.out.printf("Worker %s/%d busy: %s\n", scanClass, workerId, ((Boolean)busy).toString());
		}
		catch(Exception e) {
			System.err.println("Warning: unable to determine busy status");
			e.printStackTrace(System.err);
		}
		
		is.close();
		return busy ? 1 : 0;

	}

	public ProbeMetric collectOrThrow() throws Exception {

		HashMap<Integer,Object> values = new HashMap<Integer,Object>();
		values.put(0, getBusy());
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
		return "SCAN worker status";
	}
	
	public static void main(String[] args) {
		try {
			ScanWorkerProbe p = new ScanWorkerProbe();
			p.activate();
		}
		catch(Exception e) {
			System.err.println("Probe failed: " + e);
			e.printStackTrace(System.err);
		}
	}
}
