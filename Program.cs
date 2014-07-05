using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualBasic.FileIO;
using System.Data;

namespace GTFSRouteStopMatrix
{
    class Program
    {
        class TripTime
        {
            public Int32 trip_id { get; set; }
            public TimeSpan? stop_time { get; set; }
        }
        class TripTimes
        {
            public Int32 trip_id { get; set; }
            public TimeSpan?[] stop_times { get; set; } //indexed by stop sequence
        }
        class Stop
        {
            public Int32 route_id { get; set; }
            public String route_short_name { get; set; }
            public String route_long_name { get; set; }
            public Int32 direction_id { get; set; }
            public Int32? stop_sequence { get; set; }
            public Int32 stop_id { get; set; }
            public String stop_code { get; set; }
            public String stop_name { get; set; }
            public String stop_desc { get; set; }
            public Decimal stop_lat { get; set; }
            public Decimal stop_lon { get; set; }
            public List<TripTime> stop_times { get; set; }
            public Boolean stop_times_sorted { get; set; }
            public Stop()
            {
                stop_times = new List<TripTime>();
                stop_times_sorted = false;
            }
        }
        class StopCollection : List<Stop> { }
        static void LoadTable(DataTable table, ZipArchiveEntry entry, params DataColumn[] definedColumns)
        {
            using (var entryStream = entry.Open())
            {
                var rowNumber = 1;
                var textFieldParser = new TextFieldParser(entryStream);
                textFieldParser.TextFieldType = FieldType.Delimited;
                textFieldParser.SetDelimiters(",");
                textFieldParser.HasFieldsEnclosedInQuotes = true;
                var fields = textFieldParser.ReadFields();
                foreach (var fieldName in fields)
                {
                    var definedColumn = definedColumns.SingleOrDefault(item => item.ColumnName.Equals(fieldName));
                    if (definedColumn != null)
                        table.Columns.Add(definedColumn);
                    else
                        table.Columns.Add(fieldName);
                }
                while (!textFieldParser.EndOfData)
                {
                    Console.WriteLine(String.Format("{0}", rowNumber++));
                    fields = textFieldParser.ReadFields();
                    var newRow = table.NewRow();
                    for (var index = 0; index < fields.Length; index++)
                    {
                        try
                        {
                            if (table.Columns[index].DataType == typeof(DateTime))
                                newRow[index] = DateTime.Parse(fields[index]);
                            else if (table.Columns[index].DataType == typeof(Decimal))
                                newRow[index] = Decimal.Parse(fields[index]);
                            else if (table.Columns[index].DataType == typeof(Double))
                                newRow[index] = Double.Parse(fields[index]);
                            else if (table.Columns[index].DataType == typeof(Int32))
                                newRow[index] = Int32.Parse(fields[index]);
                            else if (table.Columns[index].DataType == typeof(TimeSpan))
                                newRow[index] = TimeSpan.Parse(fields[index]);
                            else
                                newRow[index] = fields[index];
                        }
                        catch { }
                    }
                    table.Rows.Add(newRow);
                }
            }
        }
        static void Main(string[] args)
        {
            var GTFSPath = args[0];
            var service_id = Int32.Parse(args[1]);
            var CSVPath = args[2];
            var dataSet = new DataSet();
            var routes = new DataTable();
            var stop_times = new DataTable();
            var stops = new DataTable();
            var trips = new DataTable();
            using (var zipStream = File.OpenRead(args[0]))
            {
                var zipArchive = new ZipArchive(zipStream);
                foreach (var entry in zipArchive.Entries)
                {
                    Console.WriteLine(entry.Name);
                    switch (entry.Name)
                    {
                        case "routes.txt":
                            LoadTable(routes, entry,
                            new DataColumn { ColumnName = "route_id", DataType = typeof(Int32) }
                            );
                            break;
                        case "stop_times.txt":
                            LoadTable(stop_times, entry,
                            new DataColumn { ColumnName = "trip_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "arrival_time", DataType = typeof(TimeSpan) },
                            new DataColumn { ColumnName = "departure_time", DataType = typeof(TimeSpan) },
                            new DataColumn { ColumnName = "stop_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "stop_sequence", DataType = typeof(Int32) }
                            );
                            break;
                        case "stops.txt":
                            LoadTable(stops, entry,
                            new DataColumn { ColumnName = "stop_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "stop_lat", DataType = typeof(Decimal) },
                            new DataColumn { ColumnName = "stop_lon", DataType = typeof(Decimal) }
                            );
                            break;
                        case "trips.txt":
                            LoadTable(trips, entry,
                            new DataColumn { ColumnName = "route_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "service_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "trip_id", DataType = typeof(Int32) },
                            new DataColumn { ColumnName = "direction_id", DataType = typeof(Int32) }
                            );
                            break;
                    }
                }
            }
            dataSet.Tables.Add(routes);
            dataSet.Tables.Add(stop_times);
            dataSet.Tables.Add(stops);
            dataSet.Tables.Add(trips);
            dataSet.Relations.Add(new DataRelation("routes_trips", routes.Columns["route_id"], trips.Columns["route_id"]));
            dataSet.Relations.Add(new DataRelation("trips_stop_times", trips.Columns["trip_id"], stop_times.Columns["trip_id"]));
            dataSet.Relations.Add(new DataRelation("stops_stop_times", stops.Columns["stop_id"], stop_times.Columns["stop_id"]));
            var stopCollection = new StopCollection();
            foreach (var route in routes.Rows.Cast<DataRow>())
            {
                var route_id = (Int32)route["route_id"];
                var route_short_name = (String)route["route_short_name"];
                var route_long_name = (String)route["route_long_name"];
                foreach (var direction_id in new Int32[] { 0, 1 })
                {
                    var direction_trips = route.GetChildRows("routes_trips").Cast<DataRow>().Where(item => item["service_id"].Equals(service_id) && item["direction_id"].Equals(direction_id)).ToArray();
                    if (direction_trips.Length > 0)
                    {
                        var localStopCollection = stops.Rows.Cast<DataRow>().Select(item => new Stop { route_id = route_id, route_short_name = route_short_name, route_long_name = route_long_name, direction_id = direction_id, stop_id = (Int32)item["stop_id"], stop_code = (String)item["stop_code"], stop_name = (String)item["stop_name"], stop_desc = (String)item["stop_desc"], stop_lat = (Decimal)item["stop_lat"], stop_lon = (Decimal)item["stop_lon"] }).ToList();
                        foreach (var trip in direction_trips)
                        {
                            var trip_stop_times = trip.GetChildRows("trips_stop_times").Cast<DataRow>().OrderBy(item => (Int32)item["stop_sequence"]);
                            Console.WriteLine(String.Format("{0}\t{1}\t{2}", route_short_name, direction_id, trip["trip_id"]));
                            if (localStopCollection.Any(item => item.stop_sequence.HasValue))
                            {
                                // here's the interesting part - for each subsequent trip on a route, reorder portions of the localStopCollection stops to wedge in stops that were not already in the sequence
                                // first, make sure we have an anchor point (at least one of the stop_id's in our new trip has been sequenced from a previous trip, otherwise we're lost
                                var newStopIDs = trip_stop_times.Select(item => (Int32)item["stop_id"]).ToArray();
                                if (localStopCollection.Any(item => item.stop_sequence.HasValue && newStopIDs.Contains(item.stop_id)))
                                {
                                    var pendingStops = new Queue<DataRow>();
                                    foreach (var trip_stop_time in trip_stop_times)
                                    {
                                        var stop_id = (Int32)trip_stop_time["stop_id"];
                                        var existingStop = localStopCollection.Single(item => item.stop_id.Equals(stop_id));
                                        if (existingStop.stop_sequence.HasValue)
                                        {
                                            try
                                            {
                                                existingStop.stop_times.Add(new TripTime { trip_id = (Int32)trip_stop_time["trip_id"], stop_time = (TimeSpan)trip_stop_time["departure_time"] });
                                            }
                                            catch { }
                                            var currentSequence = existingStop.stop_sequence.Value;
                                            while (pendingStops.Count > 0)
                                            {
                                                // we need to shift all the later sequences up in localStopCollection for each pending stop
                                                var upperPortion = localStopCollection.Where(item => item.stop_sequence >= currentSequence).ToList();
                                                upperPortion.ForEach(item => item.stop_sequence++);
                                                var pendingStop = pendingStops.Dequeue();
                                                var newStop = localStopCollection.Single(item => item.stop_id.Equals((Int32)pendingStop["stop_id"]));
                                                newStop.stop_sequence = currentSequence;
                                                newStop.stop_times.Add(new TripTime { trip_id = (Int32)pendingStop["trip_id"], stop_time = (TimeSpan)pendingStop["departure_time"] });
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
                                        var newStop = localStopCollection.Single(item => item.stop_id.Equals((Int32)pendingStop["stop_id"]));
                                        newStop.stop_sequence = localStopCollection.Where(item => item.stop_sequence.HasValue).Max(item => item.stop_sequence.Value) + 1;
                                        newStop.stop_times.Add(new TripTime { trip_id = (Int32)pendingStop["trip_id"], stop_time = (TimeSpan)pendingStop["departure_time"] });
                                    }

                                }
                                // else we don't know what to do yet
                            }
                            else
                            {
                                foreach (var trip_stop_time in trip_stop_times)
                                {
                                    var stop_id = (Int32)trip_stop_time["stop_id"];
                                    var stop_sequence = (Int32)trip_stop_time["stop_sequence"];
                                    var localStop = localStopCollection.Single(item => item.stop_id.Equals(stop_id));
                                    localStop.stop_sequence = stop_sequence;
                                    localStop.stop_times.Add(new TripTime { trip_id = (Int32)trip_stop_time["trip_id"], stop_time = (TimeSpan)trip_stop_time["departure_time"] });
                                }
                            }
                        }
                        var activeStopCollection = localStopCollection.Where(item => item.stop_sequence.HasValue).OrderBy(item=>item.stop_sequence).ToArray();
                        var max_stop_sequence = activeStopCollection.Max(item => item.stop_sequence.Value);
                        // build trip stop matrix
                        var trip_stop_matrix = new List<TripTimes>();
                        List<TripTimes> trip_stop_matrix_sorted = null;
                        foreach (var trip in direction_trips)
                        {
                            var tripTimes = new TripTimes { trip_id = (Int32)trip["trip_id"], stop_times = new TimeSpan?[max_stop_sequence] };
                            foreach (var activeStop in activeStopCollection)
                            {
                                var theseStops = trip.GetChildRows("trips_stop_times").Cast<DataRow>().ToList();
                                var trip_stop_time = theseStops.FirstOrDefault(item => (Int32)item["stop_id"] == activeStop.stop_id);
                                if (trip_stop_time != null)
                                {
                                    try
                                    {
                                        tripTimes.stop_times[activeStop.stop_sequence.Value - 1] = (TimeSpan)trip_stop_time["departure_time"];
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
                        if(trip_stop_matrix_sorted!=null)
                        {
                            foreach (var stop in activeStopCollection)
                            {
                                stop.stop_times = new List<TripTime>();
                                foreach (var trip in trip_stop_matrix_sorted)
                                {
                                    stop.stop_times.Add(new TripTime { trip_id = trip.trip_id, stop_time = trip.stop_times[stop.stop_sequence.Value-1].HasValue ? trip.stop_times[stop.stop_sequence.Value-1].Value : (TimeSpan?)null });
                                }
                                stop.stop_times_sorted = true;
                            }
                        }
                        stopCollection.AddRange(activeStopCollection);
                    }
                }
            }
            using (var output = File.CreateText(CSVPath))
            {
                var direction = new String[] { "Outbound", "Inbound" };
                output.WriteLine("route_short_name,route_long_name,direction,stop_sequence,stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_times");
                foreach (var stop in stopCollection.OrderBy(item => item.route_short_name).ThenBy(item => item.direction_id).ThenBy(item => item.stop_sequence))
                {
                    output.WriteLine(String.Format("\"{1}\",\"{2}\",\"{3}\",{4},{5},\"{6}\",\"{7}\",\"{8}\",{9},{10},\"{11}\"", stop.route_id, stop.route_short_name, stop.route_long_name, direction[stop.direction_id], stop.stop_sequence, stop.stop_id, stop.stop_code, stop.stop_name, stop.stop_desc, stop.stop_lat, stop.stop_lon, stop.stop_times_sorted ? String.Join(", ", stop.stop_times.Select(trip_time => trip_time.stop_time.HasValue ? trip_time.stop_time.Value.ToString() : "--:--:--").ToArray()) : String.Join(", ", stop.stop_times.OrderBy(trip_time => trip_time.stop_time).Select(trip_time => trip_time.stop_time.ToString()).ToArray())));
                }
            }
        }
    }
}
