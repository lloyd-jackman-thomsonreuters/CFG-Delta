# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 22:05:30 2017

@author: L Jackman
"""

from ftplib import FTP
import json
import sys
import os
import zipfile
import xml.etree.ElementTree as ET
import datetime as dt
from dateutil.relativedelta import relativedelta
from collections import namedtuple

if __name__ == '__main__':
    os.chdir("G:\\Custom Feeds\\LGDF\\MSCI\\")
    curdir = os.getcwd()
    #%%
    #This first part sets up the variables, such as FTP credentials and the feed specifics
    
    with open('config.json', 'r') as config:
            data = json.load(config)
            ftp_site = data["ftp"]["site"]
            user = data["ftp"]["user"]
            password = data["ftp"]["password"]
            LTSF_loc = data["ftp"]["LTSF_loc"]
            UKIns_loc = data["ftp"]["UKIns_loc"]
            fl_loc = data["ftp"]["fl_loc"]
            feed_series = data["feed"]["series"]
            feed_seq = data["feed"]["sequence"]
            feed_type = data["feed"]["type"]
            daysback = data["feed"]["daysback"]
            temp = data["temp"]
            log = data["log"]
            output = data["output"]
    
    #%% This part sets up the FTP connection and builds a list of funds on the basis of the MSCI Barra UK Insurance fund universe that will need UK Net calculations
    ftp = FTP(ftp_site, user, password)
    open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Logged in to " + ftp_site +"\n")
    
    
    
    ftp.cwd(UKIns_loc)
    folder_contents = list(ftp.mlsd())
    for item in folder_contents:
        file_name = item[0]
        if file_name.startswith("InsuranceUK"):
            UKIns_file = file_name
    
    local_filename = os.path.join(temp, UKIns_file)
    f = open(local_filename, 'wb')
    ftp.retrbinary('RETR ' + UKIns_file, f.write, 262144)
    open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Downloaded " + UKIns_file +"\n")
    f.close()
    
    zipfile.ZipFile(local_filename).extract("Performance.txt", path=temp)
    local_UKIns_perf = os.path.join(temp, "Performance.txt")
    UKIns_ids = set()
    with open(local_UKIns_perf) as p:
        for line in p:
            lipperid = line.split("\t")[0]
            UKIns_ids.add(lipperid)
        open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t UKN \t"+str(len(UKIns_ids))+" UK Insurance funds\n")
    try:
        os.remove(local_filename)
        os.remove(local_UKIns_perf)
    except Exception as err:
        print(str(err))
    #%% This part goes through the FTP listings for LTSF, checking for posted md5 checksum files with batch numbers in advance of the last processed
    ftp.cwd(LTSF_loc)
    folder_contents = list(ftp.mlsd())
    
    batches_to_process = []
    for item in folder_contents:
        file_name = item[0]
        if not file_name.endswith(".md5"): continue
        if not file_name.split("_")[0] == feed_series: continue
        if not int((file_name.split("_")[3]).split(".")[0]) > feed_seq: continue
        if not file_name.split("_")[1] == feed_type: continue
        batch = file_name.split(".")[0]
        print(batch+" to be processed")
        batches_to_process.append(batch)
    if len(batches_to_process) == 0:
        print("No LTSF batches to process in advance of the last processed: "+str(feed_seq))        
        sys.exit()
    batches_to_process = sorted(batches_to_process)
    
    #%% This part defines a dictionary of tuples containing fund lists (Gross and UK Net) for each period, resetting them with the Lipper ID header, as well as their respective date ranges
    names = []
    today = dt.datetime.today()
    
    periods = {}
    period_counts = {}
    Range = namedtuple('Range', ['start', 'end'])
    
    for n in range(1,13):
        period = str(n)+"m"
        file_name = "fund_list_"+period+".txt"
        with open(os.path.join(curdir, file_name), 'w') as o:
            o.write("LipperID\n")
        file_name_UKN = "fund_list_"+period+"UKN.txt"
        with open(os.path.join(curdir, file_name_UKN), 'w') as o:
            o.write("LipperID\n")
        start_date = today - relativedelta(months=n)
        end_date = today - relativedelta(months=(n-1))
        period_range = Range(start = start_date, end = end_date)
        count = 0
        periods[period] = (file_name, file_name_UKN, period_range)
        period_counts[period] = count
    
    for n in range(2,11):
        period = str(n)+"y"
        file_name = "fund_list_"+period+".txt"
        with open(os.path.join(curdir, file_name), 'w') as o:
            o.write("LipperID\n")
        file_name_UKN = "fund_list_"+period+"UKN.txt"
        with open(os.path.join(curdir, file_name_UKN), 'w') as o:
            o.write("LipperID\n")
        start_date = today - relativedelta(years=n)
        end_date = today - relativedelta(years=(n-1))
        period_range = Range(start = start_date, end = end_date)
        count = 0
        periods[period] = (file_name, file_name_UKN, period_range)
        period_counts[period] = count
    
    period = 'fl'
    file_name = "fund_list_"+period+".txt"
    with open(os.path.join(curdir, file_name), 'w') as o:
        o.write("LipperID\n")
    file_name_UKN = "fund_list_"+period+"UKN.txt"
    with open(os.path.join(curdir, file_name_UKN), 'w') as o:
        o.write("LipperID\n")
    start_date = dt.datetime.min
    end_date = today - relativedelta(years=(n))
    period_range = Range(start = start_date, end = end_date)
    count = 0
    periods[period] = (file_name, file_name_UKN, period_range)
    period_counts[period] = count
    
    #%% This next part downloads each of the zip files for each of the batches in turn, unzipping them, parsing their contents so as to obtain details of the start and end date of the range of prices added or modified, before then writing the Lipper ID of each asset to the appropriate fund list based on period and need for UK Net calculation
        
    for batch in batches_to_process:
        new_feed_seq= int(batch.split("_")[3])
        for file in folder_contents:
            file_name = file[0]
            if not file_name.startswith(("_").join(batch.split("_")[:-1])):
                continue
            if not file_name.endswith(".zip"):
                continue
            if file[1]["size"] == 0:
                open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t ERROR \t 0 byte file: " + file_name +"\n")
                continue
            local_filename = os.path.join(temp, file_name)
            f = open(local_filename, 'wb')
            ftp = FTP(ftp_site, user, password)
            ftp.cwd(LTSF_loc)
            ftp.retrbinary('RETR ' + file_name, f.write, 262144)
            open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Downloaded " + file_name +"\n")
            f.close()
            for zipfilename in os.listdir(temp):
                if not (zipfile.is_zipfile(temp+ '\\' + zipfilename)): continue
                print(('Unzipping ' + temp + '\\' + zipfilename))
                zipfile.ZipFile(temp+ '\\' + zipfilename).extractall(path=temp)
                for filename in os.listdir(temp):
                    if not filename.endswith('.xml'): 
                        continue
                    if filename.startswith("1"):
                        os.remove(temp+ '\\' + filename)
                        continue
                    print('Processing ' + filename)
                    ftp.voidcmd("NOOP")
                    try:
                        tree = ET.parse(temp+ '\\' + filename)
                    except ET.ParseError:
                        os.remove(os.path.join(temp, filename))
                        continue
                    root = tree.getroot()
                    ns = root.tag[:-4]
                    for fin_hist in root.iter(ns+"FinancialHistory"):
                        lipperid = fin_hist.get("Id")
                        startdate = 'None'
                        divstartdate = 'None'
                        dt_startdate = today
                        dt_divstartdate = today
                        number_of_prices = 0
                        UKN = False
                        if lipperid in UKIns_ids:
                            UKN = True
                        try:
                            startdate = fin_hist.find("./"+ns+"Prices").get("StartDate")
                            enddate = fin_hist.find("./"+ns+"Prices").get("EndDate")
                            prices  = fin_hist.find("./"+ns+"Prices")
                            number_of_prices = len(list(prices.iter()))
                            dt_startdate = dt.datetime(int(startdate[0:4]), int(startdate[5:7]), int(startdate[8:10]))
                            dt_enddate = dt.datetime(int(enddate[0:4]), int(enddate[5:7]), int(enddate[8:10]))
                        except AttributeError:
                            pass
                        try:
                            divstartdate = fin_hist.find("./"+ns+"Distributions").get("AsOfDate")
                            dt_divstartdate = dt.datetime(int(divstartdate[0:4]), int(divstartdate[5:7]), int(divstartdate[8:10]))
                        except AttributeError:
                            pass
                        if (startdate == 'None') and (divstartdate == 'None'):
                            continue
                        min_date = min([dt_startdate, dt_divstartdate])
                        max_date = max([dt_enddate, dt_divstartdate])
                        fund_range = Range(start = min_date, end = max_date)
                        if (today - dt.timedelta(days=daysback) > min_date):
                            with open(output, 'a') as output_file:
                                output_file.write(lipperid + "\t" + startdate + "\t" + enddate + "\t" + str(number_of_prices) + "\t" + divstartdate + "\n")
                            for period in periods.items():
                                period_range = period[1][2]
                                latest_start = max(fund_range.start, period_range.start)
                                earliest_end = min(fund_range.end, period_range.end)
                                overlap = (earliest_end - latest_start).days + 1
                                if overlap > 0:
                                    p = period[0]
                                    period_counts[p] += 1
                                    if lipperid in UKIns_ids:
                                        file = period[1][1]
                                    else:
                                        file = period[1][0]
                                    with open(os.path.join(curdir, file), 'a') as fundlist:
                                        fundlist.write(lipperid+"\n")
                    os.remove(os.path.join(temp, filename))
                os.remove(temp+ '\\' + zipfilename)
    #%% Logging counts and Fund List Uploading 
    for item in period_counts.items():
        open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t Period \t" + str(item[0]) + " # " + str(item[1]) +"\n")
    
       
    ftp = FTP(ftp_site, user, password)    
    ftp.cwd(fl_loc)
    
    for period in periods.values():
        for file in period[:2]:
            try:
                ftp.storbinary("STOR " + file, open(os.path.join(curdir, file), 'rb'))
                open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Uploaded " + file +"\n")
            except Exception as err:
                print(str(err))
    
    #%% Writing back configurations to JSON file
    data = {}  
    data["ftp"] = {}
    data["ftp"]["site"] = ftp_site
    data["ftp"]["user"] = user
    data["ftp"]["password"] = password
    data["ftp"]["LTSF_loc"] = LTSF_loc
    data["ftp"]["UKIns_loc"] = UKIns_loc
    data["ftp"]["fl_loc"] = fl_loc
    data["feed"] = {}
    data["feed"]["series"] = feed_series
    data["feed"]["sequence"] = new_feed_seq
    data["feed"]["type"] = feed_type
    data["feed"]["daysback"] = daysback
    data["temp"] = temp
    data["log"] = log
    data["output"] = output

    with open('config.json', 'w') as config:
        json.dump(data, config)
