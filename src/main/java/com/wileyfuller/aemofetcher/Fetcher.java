package com.wileyfuller.aemofetcher;

import com.wileyfuller.aemofetcher.model.OperationalDemandEntry;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import static java.util.stream.Collectors.toList;

/**
 * Created by wiley on 29/04/2016.
 */
public class Fetcher {


    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, SQLException, org.apache.commons.cli.ParseException {
        Class.forName("org.sqlite.JDBC");

        Fetcher fetcher = new Fetcher();
        fetcher.run(args);
    }

    public void run(String[] args) throws IOException, ParseException, SQLException, org.apache.commons.cli.ParseException {

        Options options = new Options();
        options.addOption("fetch", "Fetch data from the AEMO server, and add to DB.");
        options.addOption("writebin", "Write the data out in Binary int32 format.");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        boolean doFetch = cmd.hasOption("fetch");
        boolean doWriteBin = cmd.hasOption("writebin");
        if ((doFetch && doWriteBin) || (!doFetch && !doWriteBin)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("aemo-datafetcher", options);
            System.exit(1);
        }


        String userHome = System.getProperty("user.home");
        File userHomeDir = new File(userHome);
        File docsDir = new File(userHomeDir, "Documents");
        File binDir = new File(docsDir, "aemo-bin-data");
        binDir.mkdirs();
        File dbFile = new File(docsDir, "aemo-data.db");


        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.getCanonicalPath());
        createOperationDemandTable(connection);

        if (doFetch) {
            fetchData(connection);
        } else if (doWriteBin) {

            writeBinaryData(binDir, connection);
        }


    }


    public void fetchData(Connection connection) throws IOException, SQLException, ParseException {


        CloseableHttpClient httpclient = HttpClients.createDefault();
        String baseUrl = "http://nemweb.com.au";
        try {

            HttpGet httpget = new HttpGet(baseUrl + "/Reports/CURRENT/Operational_Demand/ACTUAL_HH/");

            System.out.println("Executing request " + httpget.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> pageResponseHandler = new ResponseHandler<String>() {

                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };

            ResponseHandler<List<String>> dataZipHandler = new ResponseHandler<List<String>>() {
                public List<String> handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
                    List<String> filesContent = new ArrayList<>();
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        try (final InputStream is = entity.getContent()) {
                            ZipInputStream zis = new ZipInputStream(is);
                            ZipEntry entry = zis.getNextEntry();

                            while (entry != null) {
                                StringBuilder sb = new StringBuilder();
                                byte[] buf = new byte[2048];
                                int len = zis.read(buf);
                                while (len != -1) {
                                    sb.append(String.valueOf(new String(buf, 0, len, Charset.forName("UTF-8"))));
                                    len = zis.read(buf);
                                }
                                filesContent.add(sb.toString());
                                entry = zis.getNextEntry();
                            }

                        }
                        return filesContent;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };


            String responseBody = httpclient.execute(httpget, pageResponseHandler);

            final Document document = Jsoup.parse(responseBody);

            Elements elements = document.select("a");


            List<String> filteredPaths = elements.stream()
                    .filter(el -> el.attr("href").contains("PUBLIC_ACTUAL"))
                    .map(el -> {
                        return el.attr("href");
                    })
//                    .limit(10)
                    .collect(toList());

            for (String p : filteredPaths) {
                System.out.println(p);
            }


            List<List<String>> collectedData = filteredPaths.stream().parallel()
                    .map(
                            path -> {
                                List<String> data = null;
                                try {
                                    data = httpclient.execute(new HttpGet(baseUrl + path), dataZipHandler);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }


                                return data;
                            }
                    )
                    .collect(toList());


            for (List<String> fileData : collectedData) {
                for (String datum : fileData) {
//                    System.out.println(datum);

                    StringReader src = new StringReader(datum);


                    for (OperationalDemandEntry entry : getEntriesFromCsv(src)) {
                        System.out.println(String.format("Time: %s, Region: %s, Demand: %s", entry.getIntervalDateTime(), entry.getRegionId(), entry.getDemand()));

                        writeOpDemandEntryToDb(entry, connection);
                    }
                }

            }


        } finally {
            httpclient.close();
        }
    }


    public List<OperationalDemandEntry> getEntriesFromCsv(Reader src) throws IOException, ParseException {
        CSVParser parser = CSVFormat.DEFAULT.parse(src);

        List<OperationalDemandEntry> entries = new ArrayList<>();
        HashMap<String, Integer> headerIndexes = new HashMap<>();
        for (CSVRecord row : parser.getRecords()) {
            String col1 = row.get(0);
            if (col1.equals("C")) {
                continue;
            } else if (col1.equals("I")) {
                int i = 0;
                for (String col : row) {
                    headerIndexes.put(col, i++);
                }
            } else {

                OperationalDemandEntry entry = mapCsvRecordToEntry(row, headerIndexes);
                entries.add(entry);

            }
        }
        return entries;
    }

    public OperationalDemandEntry mapCsvRecordToEntry(CSVRecord record, Map<String, Integer> headerIndexes) throws ParseException {
        OperationalDemandEntry entry = new OperationalDemandEntry();

        entry.setRegionId(record.get(4));
        entry.setDemand(Integer.parseInt(record.get(6)));
        entry.setType(OperationalDemandEntry.DemandType.valueOf(record.get(2)));
        entry.setRegionId(record.get(headerIndexes.get("REGIONID")));

        DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date result = df.parse(record.get(5));
        entry.setIntervalDateTime(result);

        return entry;
    }

    public OperationalDemandEntry mapResultSetToEntry(ResultSet rs) throws ParseException, SQLException {
        OperationalDemandEntry entry = new OperationalDemandEntry();

        entry.setRegionId(rs.getString("region_id"));
        entry.setDemand(rs.getInt("demand"));
        entry.setType(OperationalDemandEntry.DemandType.ACTUAL);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date result = df.parse(rs.getString("interval_datetime"));
        entry.setIntervalDateTime(result);

        return entry;
    }


    public void createOperationDemandTable(Connection conn) throws SQLException {

        boolean tableExists = false;
        try (Statement stmt = conn.createStatement();) {
            stmt.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actual_operational_demand';");
            try (ResultSet rs = stmt.getResultSet();) {
                tableExists = rs.next();
            }
        }

        if (!tableExists) {
            try (Statement stmt = conn.createStatement();) {
                stmt.execute("CREATE TABLE actual_operational_demand " +
                        "(region_id string, demand integer, interval_datetime string," +
                        " UNIQUE(region_id, interval_datetime) ON CONFLICT REPLACE)");
            }
        }


    }

    public void writeOpDemandEntryToDb(OperationalDemandEntry entry, Connection conn) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        try (PreparedStatement stmt =
                     conn.prepareStatement("INSERT INTO actual_operational_demand " +
                             "(region_id, demand, interval_datetime)" +
                             "VALUES (?, ?, ?)");) {
            stmt.setString(1, entry.getRegionId());
            stmt.setInt(2, entry.getDemand());
            stmt.setString(3, sdf.format(entry.getIntervalDateTime()));

            stmt.execute();
        }
    }


    public void writeBinaryData(File binDir, Connection conn) throws SQLException, ParseException, IOException {
        //TODO this needs to determine if any intervals are missing.
        Map<String, List<OperationalDemandEntry>> intervalsByRegion = new HashMap<>();
        List<String> regionIds = new ArrayList<>();

        try (Statement stmt =
                     conn.createStatement()) {

            stmt.execute("select DISTINCT region_id  from actual_operational_demand ");

            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    regionIds.add(rs.getString("region_id"));
                }
            }
        }

        for (String regionId : regionIds) {


            File binOutFile = new File(binDir, regionId + "-actual-operational-demand.bin");
            try (OutputStream binOutputStream = new FileOutputStream(binOutFile);) {

                try (PreparedStatement stmt =
                             conn.prepareStatement("select *  from actual_operational_demand " +
                                     "where region_id = ? " +
                                     "order by interval_datetime asc")) {
                    stmt.setString(1, regionId);
                    stmt.execute();

                    OperationalDemandEntry[] day = new OperationalDemandEntry[48];
                    LocalDate startDate = null;
                    LocalDate previousDate = null;

                    try (ResultSet rs = stmt.getResultSet()) {
                        while (rs.next()) {

                            OperationalDemandEntry entry = mapResultSetToEntry(rs);
                            LocalDateTime dt = LocalDateTime.ofInstant(entry.getIntervalDateTime().toInstant(), ZoneId.systemDefault());

                            LocalDate currDate = dt.toLocalDate();
                            if (startDate == null) {
                                startDate = currDate;
                                previousDate = currDate;
                            }

                            if (!previousDate.equals(currDate)) {
                                writeDayToOutputStream(binOutputStream, day);
                                day = new OperationalDemandEntry[48];

                            }

                            int slot = dt.getHour() * 2;
                            slot = dt.getMinute() > 0 ? slot + 1 : slot;
                            day[slot] = entry;

                            previousDate = currDate;

//                        List<OperationalDemandEntry> entries = intervalsByRegion.get(entry.getRegionId());
//                        if (entries == null) {
//                            entries = new ArrayList<>();
//                            intervalsByRegion.put(entry.getRegionId(), entries);
//                        }
//                        entries.add(entry);
                        }
                    }
                }
            }
        }


//        for (String regionId : intervalsByRegion.keySet()) {
//            System.out.println("Region: " + regionId);
//            List<OperationalDemandEntry> entries = intervalsByRegion.get(regionId);
//            File binOutFile = new File(binDir, regionId + "-actual-operational-demand.bin");
//
//            ByteBuffer buf = ByteBuffer.allocate(entries.size() * 4);
//            try (FileOutputStream out = new FileOutputStream(binOutFile);) {
//                for (OperationalDemandEntry entry : entries) {
//                    buf.putInt(entry.getDemand());
//                }
//
//                out.write(buf.array());
//            }
//
//        }
    }

    public void writeDayToOutputStream(OutputStream out, OperationalDemandEntry[] day) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(day.length * 4);
        for (OperationalDemandEntry entry : day) {
            int demand = entry != null ? entry.getDemand() : 0;
            buf.putInt(demand);
        }

        out.write(buf.array());
    }
}
