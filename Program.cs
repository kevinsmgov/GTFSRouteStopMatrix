using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

using Microsoft.VisualBasic.FileIO;

namespace GTFSRouteStopMatrix
{
    class Program
    {
        [DataContract]
        public class Geometry
        {
            [DataMember(Order = 0)]
            public string type = "Point";
            [DataMember(Order = 1)]
            public Decimal[] coordinates { get; set; }
        }
        [DataContract]
        public class Feature
        {
            [DataMember(Order = 0)]
            public string type = "Feature";
            [DataMember(Order = 1)]
            public Stop properties { get; set; }
            [DataMember(Order = 2)]
            public Geometry geometry { get; set; }
        }
        [DataContract]
        public class CRS
        {
            [DataContract]
            public class Properties
            {
                [DataMember]
                public String name = "urn:ogc:def:crs:OGC:1.3:CRS84";
            }
            [DataMember(Order = 0)]
            public string type = "name";
            [DataMember(Order = 1)]
            public Properties properties = new Properties();
        }
        [DataContract]
        public class FeatureCollection
        {
            [DataMember(Order = 0)]
            public string type = "FeatureCollection";
            [DataMember(Order = 1)]
            public CRS crs = new CRS();
            [DataMember(Order = 2)]
            public List<Feature> features { get; set; }
            public FeatureCollection()
            {
                features = new List<Feature>();
            }
        }

        [DataContract]
        public class TripTime
        {
            [DataMember]
            public Int32 trip_id { get; set; }
            public TimeSpan? stop_time { get; set; }
            [DataMember(Name = "stop_time")]
            private String stop_time_string;
            [OnSerializing]
            internal void OnSerializingMethod(StreamingContext context)
            {
                this.stop_time_string = this.stop_time.HasValue ? Microsoft.VisualBasic.Strings.Right(this.stop_time.ToString(), 8) : null;
            }
        }
        public class TripTimes
        {
            public Int32 trip_id { get; set; }
            public TimeSpan?[] stop_times { get; set; } //indexed by stop sequence
        }
        [DataContract]
        public class Stop
        {
            [DataMember(Order = 0)]
            public Int32 route_id { get; set; }
            [DataMember(Order = 1)]
            public String route_short_name { get; set; }
            [DataMember(Order = 2)]
            public String route_long_name { get; set; }
            [DataMember(Order = 3)]
            public Int32 direction_id { get; set; }
            [DataMember(Order = 4)]
            public Int32? stop_sequence { get; set; }
            [DataMember(Order = 5)]
            public Int32 stop_id { get; set; }
            [DataMember(Order = 6)]
            public String stop_code { get; set; }
            [DataMember(Order = 7)]
            public String stop_name { get; set; }
            [DataMember(Order = 8)]
            public String stop_desc { get; set; }
            [DataMember(Order = 9)]
            public Decimal stop_lat { get; set; }
            [DataMember(Order = 10)]
            public Decimal stop_lon { get; set; }
            [DataMember(Order = 11)]
            public List<TripTime> stop_times { get; set; }
            public Boolean stop_times_sorted { get; set; }
            public Stop()
            {
                stop_times = new List<TripTime>();
                stop_times_sorted = false;
            }
        }

        public class StopCollection : List<Stop> { }

        static void LoadTable(DataTable table, ZipArchiveEntry entry)
        {
            var timespanExpression = new Regex("([0-9]+):([0-9]+):([0-9]+)");
            using (var entryStream = entry.Open())
            {
                var rowNumber = 1;
                var textFieldParser = new TextFieldParser(entryStream);
                textFieldParser.TextFieldType = FieldType.Delimited;
                textFieldParser.SetDelimiters(",");
                textFieldParser.HasFieldsEnclosedInQuotes = true;
                var fieldReferences = textFieldParser.ReadFields();
                while (!textFieldParser.EndOfData)
                {
                    Console.WriteLine(String.Format("{0}", rowNumber++));
                    var fields = textFieldParser.ReadFields();
                    var newRow = table.NewRow();
                    for (var index = 0; index < table.Columns.Count; index++)
                    {
                        var fieldReference = fieldReferences.Select((value, ordinal) => new { value, ordinal }).Single(item => item.value.Equals(table.Columns[index].ColumnName)).ordinal;
                        var fieldValue = fields[fieldReference];
                        try
                        {
                            if (table.Columns[index].DataType == typeof(DateTime))
                                newRow[index] = DateTime.Parse(fieldValue);
                            else if (table.Columns[index].DataType == typeof(Decimal))
                                newRow[index] = Decimal.Parse(fieldValue);
                            else if (table.Columns[index].DataType == typeof(Double))
                                newRow[index] = Double.Parse(fieldValue);
                            else if (table.Columns[index].DataType == typeof(Int32))
                                newRow[index] = Int32.Parse(fieldValue);
                            else if (table.Columns[index].DataType == typeof(TimeSpan))
                            {
                                var timeSpanPart = fieldValue.Split(':');
                                if (timeSpanPart.Length == 3)
                                    newRow[index] = new TimeSpan(Int32.Parse(timeSpanPart[0]), Int32.Parse(timeSpanPart[1]), Int32.Parse(timeSpanPart[2]));
                            }
                            else
                                newRow[index] = fieldValue;
                        }
                        catch { }
                    }
                    table.Rows.Add(newRow);
                }
            }
        }
        static void Main(string[] args)
        {
            if (args.Length == 5)
            {
                var GTFSPath = args[0];
                var service_id = Int32.Parse(args[1]);
                var CSVPath = args[2];
                var JSONPath = args[3];
                var XMLPath = args[4];
                var dataSet = new DataSetGTFS();
                using (var zipStream = File.OpenRead(args[0]))
                {
                    var zipArchive = new ZipArchive(zipStream);
                    foreach (var entry in zipArchive.Entries)
                    {
                        Console.WriteLine(entry.Name);
                        switch (entry.Name)
                        {
                            case "routes.txt":
                                LoadTable(dataSet.route, entry);
                                break;
                            case "stop_times.txt":
                                LoadTable(dataSet.stop_time, entry);
                                break;
                            case "stops.txt":
                                LoadTable(dataSet.stop, entry);
                                break;
                            case "trips.txt":
                                LoadTable(dataSet.trip, entry);
                                break;
                        }
                    }
                }
                var stopCollection = new StopCollection();
                foreach (var route in dataSet.route.Rows.Cast<DataSetGTFS.routeRow>())
                {
                    foreach (var direction_id in new Int32[] { 0, 1 })
                    {
                        var direction_trips = route.GettripRows().Where(item => item.service_id.Equals(service_id) && item.direction_id.Equals(direction_id)).ToArray();
                        if (direction_trips.Length > 0)
                        {
                            var localStopCollection = dataSet.stop.Cast<DataSetGTFS.stopRow>().Select(item => new Stop { route_id = route.route_id, route_short_name = route.route_short_name, route_long_name = route.route_long_name, direction_id = direction_id, stop_id = item.stop_id, stop_code = item.stop_code, stop_name = item.stop_name, stop_desc = item.stop_desc, stop_lat = item.stop_lat, stop_lon = item.stop_lon }).ToList();
                            foreach (var trip in direction_trips)
                            {
                                var trip_stop_times = trip.Getstop_timeRows().OrderBy(item => item.stop_sequence);
                                Console.WriteLine(String.Format("{0}\t{1}\t{2}", route.route_short_name, direction_id, trip.trip_id));
                                if (localStopCollection.Any(item => item.stop_sequence.HasValue))
                                {
                                    // here's the interesting part - for each subsequent trip on a route, reorder portions of the localStopCollection stops to wedge in stops that were not already in the sequence
                                    // first, make sure we have an anchor point (at least one of the stop_id's in our new trip has been sequenced from a previous trip, otherwise we're lost
                                    var newStopIDs = trip_stop_times.Select(item => item.stop_id).ToArray();
                                    if (localStopCollection.Any(item => item.stop_sequence.HasValue && newStopIDs.Contains(item.stop_id)))
                                    {
                                        var pendingStops = new Queue<DataSetGTFS.stop_timeRow>();
                                        foreach (var trip_stop_time in trip_stop_times)
                                        {
                                            //var stop_id = (Int32)trip_stop_time["stop_id"];
                                            var existingStop = localStopCollection.Single(item => item.stop_id.Equals(trip_stop_time.stop_id));
                                            if (existingStop.stop_sequence.HasValue)
                                            {
                                                try
                                                {
                                                    existingStop.stop_times.Add(new TripTime { trip_id = trip_stop_time.trip_id, stop_time = trip_stop_time.departure_time });
                                                }
                                                catch { }
                                                var currentSequence = existingStop.stop_sequence.Value;
                                                while (pendingStops.Count > 0)
                                                {
                                                    // we need to shift all the later sequences up in localStopCollection for each pending stop
                                                    var upperPortion = localStopCollection.Where(item => item.stop_sequence >= currentSequence).ToList();
                                                    upperPortion.ForEach(item => item.stop_sequence++);
                                                    var pendingStop = pendingStops.Dequeue();
                                                    var newStop = localStopCollection.Single(item => item.stop_id.Equals(pendingStop.stop_id));
                                                    newStop.stop_sequence = currentSequence;
                                                    newStop.stop_times.Add(new TripTime { trip_id = pendingStop.trip_id, stop_time = pendingStop.departure_time });
                                                    currentSequence++;
                                                }
                                            }
                                            else
                                                pendingStops.Enqueue(trip_stop_time);
                                        }
                                        while (pendingStops.Count > 0)
                                        {
                                            // we need to append any stops not already represented in the existing trips
                                            var pendingStop = pendingStops.Dequeue();
                                            var newStop = localStopCollection.Single(item => item.stop_id.Equals(pendingStop.stop_id));
                                            newStop.stop_sequence = localStopCollection.Where(item => item.stop_sequence.HasValue).Max(item => item.stop_sequence.Value) + 1;
                                            newStop.stop_times.Add(new TripTime { trip_id = pendingStop.trip_id, stop_time = pendingStop.departure_time });
                                        }

                                    }
                                    // else we don't know what to do yet
                                }
                                else
                                {
                                    foreach (var trip_stop_time in trip_stop_times)
                                    {
                                        var stop_id = trip_stop_time.stop_id;
                                        var stop_sequence = trip_stop_time.stop_sequence;
                                        var localStop = localStopCollection.Single(item => item.stop_id.Equals(stop_id));
                                        localStop.stop_sequence = stop_sequence;
                                        localStop.stop_times.Add(new TripTime { trip_id = trip_stop_time.trip_id, stop_time = trip_stop_time.departure_time });
                                    }
                                }
                            }
                            var activeStopCollection = localStopCollection.Where(item => item.stop_sequence.HasValue).OrderBy(item => item.stop_sequence).ToArray();
                            var max_stop_sequence = activeStopCollection.Max(item => item.stop_sequence.Value);
                            // build trip stop matrix
                            var trip_stop_matrix = new List<TripTimes>();
                            List<TripTimes> trip_stop_matrix_sorted = null;
                            foreach (var trip in direction_trips)
                            {
                                var tripTimes = new TripTimes { trip_id = trip.trip_id, stop_times = new TimeSpan?[max_stop_sequence] };
                                foreach (var activeStop in activeStopCollection)
                                {
                                    var theseStops = trip.Getstop_timeRows().ToList();
                                    var trip_stop_time = theseStops.FirstOrDefault(item => item.stop_id == activeStop.stop_id);
                                    if (trip_stop_time != null)
                                    {
                                        try
                                        {
                                            tripTimes.stop_times[activeStop.stop_sequence.Value - 1] = trip_stop_time.departure_time;
                                            theseStops.Remove(trip_stop_time); // we need to account for looping routes (a stop may occur twice);
                                        }
                                        catch
                                        {

                                        }
                                    }
                                }
                                trip_stop_matrix.Add(tripTimes);
                            }
                            for (var index = 0; index < max_stop_sequence; index++)
                            {
                                if (trip_stop_matrix.All(item => item.stop_times[index].HasValue))
                                {
                                    trip_stop_matrix_sorted = trip_stop_matrix.OrderBy(item => item.stop_times[index].Value).ToList();
                                    break;
                                }
                            }
                            if (trip_stop_matrix_sorted != null)
                            {
                                foreach (var stop in activeStopCollection)
                                {
                                    stop.stop_times = new List<TripTime>();
                                    foreach (var trip in trip_stop_matrix_sorted)
                                    {
                                        stop.stop_times.Add(new TripTime { trip_id = trip.trip_id, stop_time = trip.stop_times[stop.stop_sequence.Value - 1].HasValue ? trip.stop_times[stop.stop_sequence.Value - 1].Value : (TimeSpan?)null });
                                    }
                                    stop.stop_times_sorted = true;
                                }
                            }
                            stopCollection.AddRange(activeStopCollection);
                        }
                    }
                }
                var direction = new String[] { "Outbound", "Inbound" };
                using (var output = File.CreateText(CSVPath))
                {
                    output.WriteLine("route_short_name,route_long_name,direction,stop_sequence,stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_times");
                    foreach (var stop in stopCollection.OrderBy(item => item.route_short_name).ThenBy(item => item.direction_id).ThenBy(item => item.stop_sequence))
                    {
                        output.WriteLine(String.Format("\"{1}\",\"{2}\",\"{3}\",{4},{5},\"{6}\",\"{7}\",\"{8}\",{9},{10},\"{11}\"",
                            stop.route_id,
                            stop.route_short_name,
                            stop.route_long_name,
                            direction[stop.direction_id],
                            stop.stop_sequence,
                            stop.stop_id,
                            stop.stop_code,
                            stop.stop_name,
                            stop.stop_desc,
                            stop.stop_lat,
                            stop.stop_lon,
                            stop.stop_times_sorted ?
                            String.Join(", ", stop.stop_times.Select(trip_time => trip_time.stop_time.HasValue ? Microsoft.VisualBasic.Strings.Right(trip_time.stop_time.Value.ToString(), 8) : "--:--:--").ToArray()) :
                            String.Join(", ", stop.stop_times.OrderBy(trip_time => trip_time.stop_time).Select(trip_time => Microsoft.VisualBasic.Strings.Right(trip_time.stop_time.Value.ToString(), 8)).ToArray())));
                    }
                }
                using (var output = File.Create(JSONPath))
                {
                    var serializer = new DataContractJsonSerializer(typeof(FeatureCollection));
                    var featureCollection = new FeatureCollection();
                    foreach (var stop in stopCollection.Where(item => item.stop_times_sorted).OrderBy(item => item.route_short_name).ThenBy(item => item.direction_id).ThenBy(item => item.stop_sequence))
                        featureCollection.features.Add(new Feature { properties = stop, geometry = new Geometry { coordinates = new Decimal[] { stop.stop_lon, stop.stop_lat } } });
                    serializer.WriteObject(output, featureCollection);
                }
                using (var xmlWriter = XmlWriter.Create(XMLPath))
                {
                    XNamespace mainNamespace = "urn:schemas-microsoft-com:office:spreadsheet";
                    XNamespace o = "urn:schemas-microsoft-com:office:office";
                    XNamespace x = "urn:schemas-microsoft-com:office:excel";
                    XNamespace ss = "urn:schemas-microsoft-com:office:spreadsheet";
                    XNamespace html = "http://www.w3.org/TR/REC-html40";

                    var spreadsheetDocument = new XDocument(new XDeclaration("1.0", "UTF-8", "yes"),
                        //new XComment(String.Format("Exported: {0}", DateTime.Now)),
                        new XProcessingInstruction("mso-application", "progid=\"Excel.Sheet\""));

                    var workbook = new XElement(mainNamespace + "Workbook",
                        new XAttribute(XNamespace.Xmlns + "html", html),
                        new XAttribute(XName.Get("ss", "http://www.w3.org/2000/xmlns/"), ss),
                        new XAttribute(XName.Get("o", "http://www.w3.org/2000/xmlns/"), o),
                        new XAttribute(XName.Get("x", "http://www.w3.org/2000/xmlns/"), x),
                        new XAttribute(XName.Get("xmlns", ""), mainNamespace),
                        new XElement(o + "OfficeDocumentSettings", new XAttribute(XName.Get("xmlns", ""), o)),
                        new XElement(x + "ExcelWorkbook", new XAttribute(XName.Get("xmlns", ""), x)));

                    var routeCollection = stopCollection.Where(item => item.stop_times_sorted).GroupBy(item => item.route_short_name).Select(directions => new { route_short_name = directions.Key, directions = directions.GroupBy(item => item.direction_id).Select(groupDirection => new { direction_id = groupDirection.Key, stops = groupDirection }) });
                    foreach (var thisRoute in routeCollection)
                    {
                        foreach (var thisDirection in thisRoute.directions)
                        {
                            var worksheet = new XElement(mainNamespace + "Worksheet", new XAttribute(ss + "Name", String.Format("{0} - {1}", thisRoute.route_short_name, direction[thisDirection.direction_id])));
                            var table = new XElement(mainNamespace + "Table");
                            {
                                var headerRow = new XElement(mainNamespace + "Row");
                                foreach (var stop in thisDirection.stops)
                                {
                                    var cell = new XElement(mainNamespace + "Cell", new XElement(mainNamespace + "Data", new XAttribute(ss + "Type", "String"), stop.stop_desc));
                                    headerRow.Add(cell);
                                }
                                table.Add(headerRow);
                            }
                            var maxSequence = thisDirection.stops.First().stop_times.Count;
                            for (var index = 0; index < maxSequence; index++)
                            {
                                var row = new XElement(mainNamespace + "Row");
                                foreach (var stop in thisDirection.stops)
                                {
                                    var stop_time = stop.stop_times[index];
                                    var cell = new XElement(mainNamespace + "Cell", new XElement(mainNamespace + "Data", new XAttribute(ss + "Type", "String"), stop_time.stop_time.HasValue ? Microsoft.VisualBasic.Strings.Right(stop_time.stop_time.Value.ToString(), 8) : "--:--:--"));
                                    row.Add(cell);
                                }
                                table.Add(row);
                            }
                            worksheet.Add(table);
                            workbook.Add(worksheet);
                        }
                    }
                    spreadsheetDocument.Add(workbook);
                    spreadsheetDocument.WriteTo(xmlWriter);
                }
            }
            else
                Console.WriteLine("USAGE: [GTFS.zip path] [service id] [output csv path] [output json path] [output xml path]");
        }
    }
}
