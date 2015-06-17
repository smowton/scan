
import java.util.HashMap;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.File;
import java.io.FileReader;
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

	private double doubleFromFile(String fname, double def) {
		
		try {
			FileReader fr = new FileReader(fname);
			StringWriter sw = new StringWriter();

			char[] buf = new char[4096];
		
			int lastRead;

			while((lastRead = fr.read(buf, 0, 4096)) != -1)
				sw.write(buf, 0, lastRead);

			fr.close();
			sw.close();
			return Double.parseDouble(sw.toString());
		}
		catch(IOException e) {
			return def;
		}
		
	}

	public ScanWorkerProbe(String name, int freq) throws Exception {

		super(name, freq);
		this.addProbeProperty(0, "idleCoresProp", ProbePropertyType.DOUBLE,"", "Proportion of cores idle");
		this.addProbeProperty(0, "idleMemProp", ProbePropertyType.DOUBLE,"", "Proportion of memory idle");

	}
	
	public ScanWorkerProbe() throws Exception {
		this(DEFAULT_PROBE_NAME, DEFAULT_SAMPLING_PERIOD);
	}

	public ProbeMetric collectOrThrow() throws Exception {

		HashMap<Integer,Object> values = new HashMap<Integer,Object>();
		values.put(0, doubleFromFile("/tmp/scan_idle_cores", 0));
		values.put(1, doubleFromFile("/tmp/scan_idle_mem", 0));
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
