using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Microsoft.VisualBasic.FileIO;

namespace GTFS
{
    public class GTFS
    {
        private void LoadTable(DataTable table, Stream stream)
        {
            var timespanExpression = new Regex("([0-9]+):([0-9]+):([0-9]+)");
            var parseDictionary = new Dictionary<Type, Delegate>();
            parseDictionary.Add(typeof(Boolean), new Func<String, Boolean>(fieldValue => Convert.ToBoolean(Int32.Parse(fieldValue))));
            parseDictionary.Add(typeof(DateTime), new Func<String, DateTime>(fieldValue => new DateTime(Int32.Parse(fieldValue.Substring(0, 4)), Int32.Parse(fieldValue.Substring(4, 2)), Int32.Parse(fieldValue.Substring(6, 2)))));
            parseDictionary.Add(typeof(Decimal), new Func<String, Decimal>(fieldValue => Decimal.Parse(fieldValue)));
            parseDictionary.Add(typeof(Int32), new Func<String, Int32>(fieldValue => Int32.Parse(fieldValue)));
            parseDictionary.Add(typeof(TimeSpan), new Func<String, Object>(fieldValue => { try { var timeSpanPart = fieldValue.Split(':'); return new TimeSpan(Int32.Parse(timeSpanPart[0]), Int32.Parse(timeSpanPart[1]), Int32.Parse(timeSpanPart[2])); } catch { return DBNull.Value; } }));
            var textFieldParser = new TextFieldParser(stream);
            textFieldParser.TextFieldType = FieldType.Delimited;
            textFieldParser.SetDelimiters(",");
            textFieldParser.HasFieldsEnclosedInQuotes = true;
            var fieldReferences = textFieldParser.ReadFields();
            while (!textFieldParser.EndOfData)
            {
                var fields = textFieldParser.ReadFields();
                var newRow = table.NewRow();
                for (var index = 0; index < fieldReferences.Length; index++)
                {
                    var fieldReference = fieldReferences[index];
                    var fieldValue = fields[index];
                    if (table.Columns.Contains(fieldReference))
                    {
                        if (parseDictionary.ContainsKey(table.Columns[fieldReference].DataType))
                            newRow[fieldReference] = parseDictionary[table.Columns[fieldReference].DataType].DynamicInvoke(fieldValue);
                        else
                            newRow[fieldReference] = fieldValue;
                    }
                }
                table.Rows.Add(newRow);
            }
        }
        private Boolean isTimePoint(String service_id, String route_id, String direction_id, String stop_id)
        {
            return GTFSset.time_points.Any(item => item.service_id.Equals(service_id) && item.route_id.Equals(route_id) && item.direction_id.Equals(direction_id) && item.stop_id.Equals(stop_id));
        }
        public GTFS(String zipPath)
        {
            GTFSset = new GTFSset();
            using (var zipStream = File.OpenRead(zipPath))
            {
                using (var zipArchive = new ZipArchive(zipStream))
                {
                    foreach (var entry in zipArchive.Entries)
                    {
                        using (var stream = entry.Open())
                        {
                            var targetTable = GTFSset.Tables[entry.Name];
                            if (targetTable != null)
                                LoadTable(targetTable, stream);
                        }
                    }
                }
            }
        }
        public GTFSset GTFSset { get; set; }
        public class RouteMatrixCalendar
        {
            public RouteMatrixCalendar()
            {
                Routes = new List<RouteMatrixRoute>();
            }
            public String service_id { get; set; }
            public GTFSset._calendar_txtRow calendar_txt { get; set; }
            public List<RouteMatrixRoute> Routes { get; set; }
        }
        public class RouteMatrixRoute
        {
            public RouteMatrixRoute()
            {
                Directions = new List<RouteMatrixDirection>();
            }
            public String route_id { get; set; }
            public GTFSset._routes_txtRow route_txt { get; set; }
            public List<RouteMatrixDirection> Directions { get; set; }
        }
        public class RouteMatrixDirection
        {
            public RouteMatrixDirection()
            {
                Stops = new LinkedList<RouteMatrixStop>();
            }
            public String direction_id { get; set; }
            public LinkedList<RouteMatrixStop> Stops { get; set; }
        }
        public class RouteMatrixStop
        {
            public RouteMatrixStop()
            {
                time_point = false;
                stop_times_txtRow = new List<GTFSset._stop_times_txtRow>();
                StopTrips = new List<RouteMatrixStopTrip>();
            }
            public Int32? stop_sequence { get; set; }
            public String stop_id { get; set; }
            public Boolean time_point { get; set; }
            public GTFSset._stops_txtRow stops_txt { get; set; }
            public List<GTFSset._stop_times_txtRow> stop_times_txtRow { get; set; }
            public List<RouteMatrixStopTrip> StopTrips { get; set; }
        }
        public class RouteMatrixStopTrip
        {
            public Int32? trip_sequence { get; set; }
            public String trip_id { get; set; }
            public GTFSset._trips_txtRow trips_txt { get; set; }
            public GTFSset._stop_times_txtRow stop_times_txt { get; set; }
        }
        public void loadTimePoints(Stream stream)
        {
            LoadTable(GTFSset.time_points, stream);
        }
        public List<RouteMatrixCalendar> getRouteMatrix()
        {
            var calendarMatrix = new List<RouteMatrixCalendar>();
            foreach (var calendar_txtRow in GTFSset._calendar_txt.Rows.Cast<GTFSset._calendar_txtRow>())
            {
                var thisCalendar = new RouteMatrixCalendar { service_id = calendar_txtRow.service_id, calendar_txt = calendar_txtRow };
                calendarMatrix.Add(thisCalendar);
                foreach (var route_textRow in GTFSset._routes_txt.Rows.Cast<GTFSset._routes_txtRow>())
                {
                    var thisRoute = new RouteMatrixRoute { route_id = route_textRow.route_id, route_txt = route_textRow };
                    thisCalendar.Routes.Add(thisRoute);
                    foreach (var direction_id in new String[] { "0", "1" })
                    {
                        var thisDirection = new RouteMatrixDirection { direction_id = direction_id };
                        thisRoute.Directions.Add(thisDirection);
                        var theseTrips_txt = route_textRow._Gettrips_txtRows().Where(item => item.service_id.Equals(calendar_txtRow.service_id) && item.direction_id.Equals(direction_id)).ToList();
                        foreach (var trip in theseTrips_txt)
                        {
                            var stop_times = trip._Getstop_times_txtRows().OrderBy(item => item.stop_sequence);
                            if (thisDirection.Stops.Count > 0)
                            {
                                // interpose new trip stops
                                var pending_stop_times = new Queue<GTFSset._stop_times_txtRow>();
                                foreach (var stop_time in stop_times)
                                {
                                    var existingStop = thisDirection.Stops.SingleOrDefault(item => item.stop_id.Equals(stop_time.stop_id));
                                    if (existingStop != null)
                                    {
                                        existingStop.stop_times_txtRow.Add(stop_time);
                                        while (pending_stop_times.Count > 0)
                                        {
                                            var pending_stop_time = pending_stop_times.Dequeue();
                                            var insertPoint = thisDirection.Stops.Find(existingStop);
                                            thisDirection.Stops.AddBefore(insertPoint, new RouteMatrixStop
                                            {
                                                stop_id = pending_stop_time.stop_id,
                                                stops_txt = pending_stop_time.stopsRow,
                                                time_point = isTimePoint(thisCalendar.service_id, thisRoute.route_id, thisDirection.direction_id, pending_stop_time.stop_id),
                                                stop_times_txtRow = new List<GTFSset._stop_times_txtRow> { pending_stop_time }
                                            });
                                        }
                                    }
                                    else
                                        pending_stop_times.Enqueue(stop_time);
                                }
                                while (pending_stop_times.Count > 0)
                                {
                                    var pending_stop_time = pending_stop_times.Dequeue();
                                    thisDirection.Stops.AddLast(new RouteMatrixStop
                                    {
                                        stop_id = pending_stop_time.stop_id,
                                        stops_txt = pending_stop_time.stopsRow,
                                        time_point = isTimePoint(thisCalendar.service_id, thisRoute.route_id, thisDirection.direction_id, pending_stop_time.stop_id),
                                        stop_times_txtRow = new List<GTFSset._stop_times_txtRow> { pending_stop_time }
                                    });
                                }

                            }
                            else
                            {
                                foreach (var stop_time in stop_times)
                                {
                                    thisDirection.Stops.AddLast(new RouteMatrixStop
                                    {
                                        stop_id = stop_time.stop_id,
                                        stops_txt = stop_time.stopsRow,
                                        time_point = isTimePoint(thisCalendar.service_id, thisRoute.route_id, thisDirection.direction_id, stop_time.stop_id),
                                        stop_times_txtRow = new List<GTFSset._stop_times_txtRow> { stop_time }
                                    });
                                }
                            }
                        }

                        // assign stop sequence value to linked list items
                        thisDirection.Stops.Select((stop, sequence) => new { stop, sequence }).ToList().ForEach(item => item.stop.stop_sequence = item.sequence + 1);

                        // remove duplicate stops (e.g. circular routes)
                        thisDirection.Stops.ToList().ForEach(thisStop =>
                        {
                            for (var index = 1; index < thisStop.stop_times_txtRow.Count; index++)
                            {
                                var thisstop_times_txtRow = thisStop.stop_times_txtRow[index];
                                var previousstop_times_txtRow = thisStop.stop_times_txtRow.Take(index);
                                if (previousstop_times_txtRow.Any(item => item.trip_id.Equals(thisstop_times_txtRow.trip_id)))
                                {
                                    thisStop.stop_times_txtRow.Remove(thisstop_times_txtRow);
                                    index--;
                                }
                            }
                        });

                        // populate StopTrips
                        thisDirection.Stops.ToList().ForEach(thisStop =>
                        {
                            thisStop.StopTrips = new List<RouteMatrixStopTrip>();
                            foreach (var thisTrips_txt in theseTrips_txt)
                            {
                                thisStop.StopTrips.Add(new RouteMatrixStopTrip
                                {
                                    trip_id = thisTrips_txt.trip_id,
                                    stop_times_txt = thisStop.stop_times_txtRow.SingleOrDefault(item => item.trip_id.Equals(thisTrips_txt.trip_id)),
                                    trips_txt = thisTrips_txt
                                });
                            }
                        });

                        // find a stop with all trips represented and sort the trips on that stop
                        List<RouteMatrixStopTrip> sortedTrips = null;
                        for (var index = 0; index < thisDirection.Stops.Count; index++)
                        {
                            if (thisDirection.Stops.ElementAt(index).StopTrips.All(item => item.stop_times_txt != null))
                            {
                                sortedTrips = thisDirection.Stops.ElementAt(index).StopTrips.OrderBy(item => item.stop_times_txt.departure_time).Select((item, ordinal) => new RouteMatrixStopTrip { trip_sequence = ordinal, trip_id = item.trip_id, trips_txt = item.trips_txt }).ToList();
                                break;
                            }
                        }
                        if (sortedTrips != null)
                        {
                            foreach (var thisStop in thisDirection.Stops)
                            {
                                thisStop.StopTrips = sortedTrips.Select(sortedTrip => new RouteMatrixStopTrip
                                {
                                    trip_sequence = sortedTrip.trip_sequence,
                                    stop_times_txt = thisStop.stop_times_txtRow.SingleOrDefault(stop_times_txt => stop_times_txt.trip_id.Equals(sortedTrip.trip_id)),
                                    trips_txt = sortedTrip.trips_txt
                                }).ToList();
                            }
                        }
                    }
                }
            }
            return calendarMatrix;
        }
    }
}
