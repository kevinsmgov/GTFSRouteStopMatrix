using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.VisualBasic;

namespace GTFSRouteStopMatrix
{


    public partial class DataSetGTFS
    {
        partial class calendarDataTable
        {
        }
        partial class calendarRow
        {
            public Boolean AnyDaysSelected
            {
                get
                {
                    var alldays = new Boolean[] { this.monday, this.tuesday, this.wednesday, this.thursday, this.friday, this.saturday, this.sunday };
                    var selecteddays = alldays.Where(item => item).ToArray();
                    return selecteddays.Length > 0;
                }
            }
            public String[] SelectedDays
            {

                get
                {
                    var theseDays = new List<String>();
                    for (var dayIndex = 0; dayIndex < 7; dayIndex++)
                    {
                        if ((Boolean)this[DateAndTime.WeekdayName(dayIndex + 1).ToLower()])
                            theseDays.Add(DateAndTime.WeekdayName(dayIndex + 1));
                    }
                    return theseDays.ToArray();
                }
            }
        }
    }

}
