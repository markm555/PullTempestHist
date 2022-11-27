using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace PullTempestHist
{

    /*  T-SQL to Create Weather Table
     *  --Create database Weather3

            Use Weather3
            Drop table if exists history

            create table history
            (
	            epoch datetime ,
	            windlull float,
	            windavg  float null,
	            windgust float null,
	            winddir float null,
	            windsample float null,
	            pressure float null,
	            airtemp float null,
	            relhumidity float null,
	            illuminance float null,
	            uv float null,
	            solar float null,
	            rain float null,
	            preciptype float null,
	            strikedist float null,
	            strikecount float null,
	            battery float null,
	            reptint float null,
	            dayrain float null,
	            ncrainacc float null,
	            dayrainacc float null,
	            precipatype float null
            )
    */

    /* T-SQL Agent Job to create a HiLo Temperature table from your history table.  Schedule this to run once a day preferabley shortly after midnight.
     * 
     USE [Weather]
        GO

        IF OBJECT_ID (N'dbo.HiLo', N'U') IS NOT NULL
        DROP TABLE [dbo].[HiLo]
        GO

        CREATE TABLE [dbo].[HiLo](
	        [date] [datetime] NULL,
	        [High] [int] NULL,
	        [Low] [int] NULL)

        DECLARE @DATE as date;
        DECLARE @Hi as INT;
        DECLARE @Lo as INT;
        DECLARE @DateCursor as CURSOR;

        Set @DateCursor = CURSOR FOR
        select DISTINCT(Convert (date, epoch)) from History

        OPEN @DateCursor;
        FETCH NEXT FROM @DateCursor INTO @DATE

        WHILE @@FETCH_STATUS = 0
        BEGIN

	        Select @DATE = Convert(date,epoch), @Hi = MAX(airtemp), @Lo = MIN(airtemp) from history WHERE Convert(date,epoch) = @DATE GROUP BY Convert(date,epoch)


	        INSERT INTO HiLo VALUES ( @DATE, @Hi, @Lo )

	        FEtCH NEXT FROM @DateCursor INTO @DATE

        END

        */
    class Program

    {

        //0 - Epoch(Seconds UTC
        //1 - Wind Lull(m/s)
        //2 - Wind Avg(m/s)
        //3 - Wind Gust(m/s)
        //4 - Wind Direction(degrees)
        //5 - Wind Sample Interval(seconds)
        //6 - Pressure(MB)
        //7 - Air Temperature(C)
        //8 - Relative Humidity(%)
        //9 - Illuminance(lux)
        //10 - UV(index)
        //11 - Solar Radiation(W/m^2)
        //12 - Rain Accumulation(mm)
        //13 - Precipitation Type(0 = none, 1 = rain, 2 = hail, 3 = rain + hail (experimental))
        //14 - Average Strike Distance(km)
        //15 - Strike Count
        //16 - Battery(volts)
        //17 - Report Interval(minutes)
        //18 - Local Day Rain Accumulation(mm)
        //19 - NC Rain Accumulation(mm)
        //20 - Local Day NC Rain Accumulation(mm)
        //21 - Precipitation Aanalysis Type(0 = none, 1 = Rain Check with user display on, 2 = Rain Check with user display off)

        public class history
        {

            public Int64 epoch { get; set; }
            public double windlull { get; set; }
            public double windavg { get; set; }
            public double windgust { get; set; }
            public double winddir { get; set; }
            public double windsample { get; set; }
            public double pressure { get; set; }
            public double airtemp { get; set; }
            public double relhumidity { get; set; }
            public double illuminance { get; set; }
            public double uv { get; set; }
            public double solar { get; set; }
            public double rain { get; set; }
            public double preciptype { get; set; }
            public double strikedist { get; set; }
            public double strikecount { get; set; }
            public double battery { get; set; }
            public double reptint { get; set; }
            public double dayrain { get; set; }
            public double ncrainacc { get; set; }
            public double dayrainacc { get; set; }
            public double precipatype { get; set; }
        }
        /*
          Fill in your values between these lines.  These are the only changes you need to make to get this to work
        _______________________________________________________________________________________________________________________________
        */
        private static string WeatherToken = "<Your Token>";
        private static string DeviceID = "<Your Device ID>";
        //private static long StartTime = <Epoch Start Teim>;    /  Used for epoch dates  \
        //private static long StopTime = StartTime + 86400; 	 \  rather than days      /
        private static string dbserver = "<Your Database Server>";
        private static string database = "Weather";
        private static int sday = 1;   //  /  These values are the days back you want to pull history with a start and an end day  \
        private static int eday = 2;   //  \  Today is 0, yesterday is 1 etc.  The values as set will pull just yesterdays history /
        /*
        _______________________________________________________________________________________________________________________________
        */


        static SqlConnection SQLDBconnect()
        /*
         * Set up a connection to SQL Database
         */
        {
            var localcon2 = new SqlConnection();
            localcon2.ConnectionString = "Data Source="+ dbserver +";Initial Catalog=" + database + ";Integrated Security = True";
            localcon2.Open();

            return localcon2;
        }

        static void Main(string[] args)
        {
            int i = sday;  // initialize counter.  This is used in the api call for number of days back in day_offset

            // Set up connection to SQL Database
            SqlConnection con2;
            con2 = SQLDBconnect();



            while (i < eday) // loop through 500 days
            {
                // commented url is used for epoch time to get more granulary hist other than by day.  Can pull back a single minute of history using this method.
                //var url = "https://swd.weatherflow.com/swd/rest/observations/device/152639?time_start=" + StartTime + "&time_end=" + StopTime + "&token=" + WeatherToken;
                var url = "https://swd.weatherflow.com/swd/rest/observations/device/" + DeviceID + "?day_offset=" + i + "&token=" + WeatherToken;

                try
                {

                    //make the api call to pull history for each day in the loop.  This returns a comma seperated array with data for each minute of the day.

                    var httpRequest = (HttpWebRequest)WebRequest.Create(url);
                    httpRequest.Accept = "application/json";
                    var httpResponse = (HttpWebResponse)httpRequest.GetResponse();
                    Console.WriteLine(httpRequest);
                    using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                    {
                        // parse the json and remove null values and replace them with a 0.
                        var result = streamReader.ReadToEnd();
                        JToken token1 = JToken.Parse(result);
                        JArray obs = (JArray)token1.SelectToken("obs");
                        string history = Convert.ToString(obs);
                        history = history.Replace("null", "0");
                        JArray h = JArray.Parse(history);

                        // Loop through the comma seperated list and pull out individual values and buld the record using the data contract above.
                        foreach (JToken row in h)
                        {
                            history record = new history
                            {
                                epoch = Convert.ToInt64(row[0]),
                                windlull = Convert.ToDouble(row[1]),
                                windavg = Convert.ToDouble(row[2]),
                                windgust = Convert.ToInt64(row[3]),
                                winddir = Convert.ToDouble(row[4]),
                                windsample = Convert.ToDouble(row[5]),
                                pressure = Convert.ToDouble(row[6]),
                                airtemp = Convert.ToDouble(row[7]),
                                relhumidity = Convert.ToDouble(row[8]),
                                illuminance = Convert.ToDouble(row[9]),
                                uv = Convert.ToDouble(row[10]),
                                solar = Convert.ToDouble(row[11]),
                                rain = Convert.ToDouble(row[12]),
                                preciptype = Convert.ToDouble(row[13]),
                                strikedist = Convert.ToDouble(row[14]),
                                strikecount = Convert.ToDouble(row[15]),
                                battery = Convert.ToDouble(row[16]),
                                reptint = Convert.ToDouble(row[17]),
                                dayrain = Convert.ToDouble(row[18]),
                                ncrainacc = Convert.ToDouble(row[19]),
                                dayrainacc = Convert.ToDouble(row[20]),
                                precipatype = Convert.ToDouble(row[21]),

                            };

                            //Convert UTC time returned by Tempest to your local time zone
                            var dateTime = DateTimeOffset.FromUnixTimeSeconds(record.epoch).ToLocalTime();
                            //Convert airtemp from celcius to farenheight
                            var ftemp = (record.airtemp * 9) / 5 + 32;

                            Console.WriteLine("Date: " + dateTime + "|" + "Temp" + ftemp);

                            // Create a SQL Command to SQL Server
                            SqlCommand cmd2 = new SqlCommand();
                            cmd2.Connection = con2;

                            // Open conneciton to SQL Server
                            try
                            {
                                con2.Open();
                            }
                            catch { }

                            // Build record to write to database and write it using ExecuteNonQuery.

                            cmd2.CommandText = "INSERT INTO history (epoch,windlull,windavg,windgust,winddir,windsample,pressure,airtemp,relhumidity,illuminance,uv,solar,rain,preciptype,strikedist,strikecount,battery,reptint,dayrain,ncrainacc,dayrainacc,precipatype) VALUES(@param1,@param2,@param3,@param4,@param5,@param6,@param7,@param8,@param9,@param10,@param11,@param12,@param13,@param14,@param15,@param16,@param17,@param18,@param19,@param20,@param21,@param22)";

                            cmd2.Parameters.AddWithValue("@param1", dateTime);
                            cmd2.Parameters.AddWithValue("@param2", record.windlull);
                            cmd2.Parameters.AddWithValue("@param3", record.windavg);
                            cmd2.Parameters.AddWithValue("@param4", record.windgust);
                            cmd2.Parameters.AddWithValue("@param5", record.winddir);
                            cmd2.Parameters.AddWithValue("@param6", record.windsample);
                            cmd2.Parameters.AddWithValue("@param7", record.pressure);
                            cmd2.Parameters.AddWithValue("@param8", ftemp);
                            cmd2.Parameters.AddWithValue("@param9", record.relhumidity);
                            cmd2.Parameters.AddWithValue("@param10", record.illuminance);
                            cmd2.Parameters.AddWithValue("@param11", record.uv);
                            cmd2.Parameters.AddWithValue("@param12", record.solar);
                            cmd2.Parameters.AddWithValue("@param13", record.rain);
                            cmd2.Parameters.AddWithValue("@param14", record.preciptype);
                            cmd2.Parameters.AddWithValue("@param15", record.strikedist);
                            cmd2.Parameters.AddWithValue("@param16", record.strikecount);
                            cmd2.Parameters.AddWithValue("@param17", record.battery);
                            cmd2.Parameters.AddWithValue("@param18", record.reptint);
                            cmd2.Parameters.AddWithValue("@param19", record.dayrain);
                            cmd2.Parameters.AddWithValue("@param20", record.ncrainacc);
                            cmd2.Parameters.AddWithValue("@param21", record.dayrainacc);
                            cmd2.Parameters.AddWithValue("@param22", record.precipatype);

                            try
                            {
                                cmd2.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(DateTime.Now + ": " + "Connection timeout retrying..." + ex);
                                con2.Close();
                                con2.Open();
                                cmd2.ExecuteNonQuery();
                            }
                        }
                    }
                }
                catch { }
                i = i + 1;

                // use with commented url command for epoch time.  86400 = 1 day.
                //StartTime = StopTime;
                //StopTime = StartTime + 86400;
            }
        }
    }
}

