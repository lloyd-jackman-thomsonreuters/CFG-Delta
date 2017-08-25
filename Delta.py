# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 22:05:30 2017

@author: panther
"""

from ftplib import FTP
import json
import sys
import os
import zipfile
import xml.etree.ElementTree as ET
import datetime as dt
from dateutil.relativedelta import relativedelta

if __name__ == '__main__':
    os.chdir("G:\\Custom Feeds\\LGDF\\MSCI\\")
    
    #This first part sets up the variables, such as FTP credentials and the feed specifics
    
    with open('config.json', 'r') as config:
            data = json.load(config)
            ftp_site = data["ftp"]["site"]
            user = data["ftp"]["user"]
            password = data["ftp"]["password"]
            LTSF_loc = data["ftp"]["LTSF_loc"]
            fl_loc = data["ftp"]["fl_loc"]
            feed_series = data["feed"]["series"]
            feed_seq = data["feed"]["sequence"]
            feed_type = data["feed"]["type"]
            daysback = data["feed"]["daysback"]
            temp = data["temp"]
            log = data["log"]
            output = data["output"]
            output3m = data["output3m"]
            output1y = data["output1y"]
            output3y = data["output3y"]
            outputfl = data["outputfl"]
    
    ftp = FTP(ftp_site, user, password)
    ftp.cwd(LTSF_loc)
    open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Logged in to " + ftp_site +"\n")
    folder_contents = list(ftp.mlsd())
    
    #We then check for md5 checksum files, the last file to be delivered in LGDF deliveries, and create a list of batches to be processed
    
    batches_to_process = []
    for item in folder_contents:
        file_name = item[0]
        if not file_name.endswith(".md5"): continue
        if not file_name.split("_")[0] == feed_series: continue
        if not int((file_name.split("_")[3]).split(".")[0]) > feed_seq: continue
        if not file_name.split("_")[1] == feed_type: continue
        batch = file_name.split(".")[0]
        print(batch)
        batches_to_process.append(batch)
    if len(batches_to_process) == 0: sys.exit()
    batches_to_process = sorted(batches_to_process)
    
    #This next part downloads each of the zip files for each of the batches in turn, unzipping them, parsing their contents --?
    names = []
    today = dt.datetime.today()
    
    with open(output, 'w') as output_file:
        output_file.write("Lipper ID\tPrice Start Date\tNumber of Prices\tDividend As Of Date\n")
    
    with open(output3m, 'w') as output_file:
        output_file.write("Lipper ID\n")
    
    with open(output1y, 'w') as output_file:
        output_file.write("Lipper ID\n")
    
    with open(output3y, 'w') as output_file:
        output_file.write("Lipper ID\n")
    
    with open(outputfl, 'w') as output_file:
        output_file.write("Lipper ID\n")
    
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
            ftp.retrbinary('RETR ' + file_name, f.write, 262144)
            open(log, 'a').write(str(dt.datetime.utcnow().strftime("%d-%m-%Y %H:%M")) + "\t FTP \t Downloaded " + file_name +"\n")
            f.close()
            for zipfilename in os.listdir(temp):
                if not (zipfile.is_zipfile(temp+ '\\' + zipfilename)): continue
                print(('Unzipping ' + temp + '\\' + zipfilename))
                zipfile.ZipFile(temp+ '\\' + zipfilename).extractall(path=temp)
                os.remove(temp+ '\\' + zipfilename)
                for filename in os.listdir(temp):
                    if not filename.endswith('.xml'): 
                        continue
                    if filename.startswith("1"):
                        os.remove(temp+ '\\' + filename)
                        continue
                    print('Processing ' + filename)
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
                        try:
                            startdate = fin_hist.find("./"+ns+"Prices").get("StartDate")
                            prices  = fin_hist.find("./"+ns+"Prices")
                            number_of_prices = len(list(prices.iter()))
                            dt_startdate = dt.datetime(int(startdate[0:4]), int(startdate[5:7]), int(startdate[8:10]))
                        except AttributeError:
                            pass
                        try:
                            divstartdate = fin_hist.find("./"+ns+"Distributions").get("AsOfDate")
                            dt_divstartdate = dt.datetime(int(divstartdate[0:4]), int(divstartdate[5:7]), int(divstartdate[8:10]))
                        except AttributeError:
                            pass
                        if (startdate == 'None') and (divstartdate == 'None'):
                            continue
                        if (today - dt.timedelta(days=daysback) > min([dt_startdate, dt_divstartdate])):
                            with open(output, 'a') as output_file:
                                output_file.write(lipperid + "\t" + startdate + "\t" + str(number_of_prices) + "\t" + divstartdate + "\n")
                            if (today - relativedelta(months=3) < min([dt_startdate, dt_divstartdate])):
                                with open(output3m, 'a') as output_file:
                                    output_file.write(lipperid + "\n")
                            elif (today - relativedelta(months=12) < min([dt_startdate, dt_divstartdate])):
                                with open(output1y, 'a') as output_file:
                                    output_file.write(lipperid + "\n")
                            elif (today - relativedelta(months=36) < min([dt_startdate, dt_divstartdate])):
                                with open(output3y, 'a') as output_file:
                                    output_file.write(lipperid + "\n")
                            else:
                                with open(outputfl, 'a') as output_file:
                                    output_file.write(lipperid + "\n")
                    os.remove(os.path.join(temp, filename))
        
    ftp.cwd(fl_loc)
    
    r = output3m.split("\\")[-1]
    ftp.storbinary("STOR "+r, open(output3m, 'rb'))
    
    r = output1y.split("\\")[-1]
    ftp.storbinary("STOR "+r, open(output1y, 'rb'))
    
    r = output3y.split("\\")[-1]
    ftp.storbinary("STOR "+r, open(output3y, 'rb'))
    
    r = outputfl.split("\\")[-1]
    ftp.storbinary("STOR "+r, open(outputfl, 'rb'))
    
    data = {}  
    data["ftp"] = {}
    data["ftp"]["site"] = ftp_site
    data["ftp"]["user"] = user
    data["ftp"]["password"] = password
    data["ftp"]["LTSF_loc"] = LTSF_loc
    data["ftp"]["fl_loc"] = fl_loc
    data["feed"] = {}
    data["feed"]["series"] = feed_series
    data["feed"]["sequence"] = new_feed_seq
    data["feed"]["type"] = feed_type
    data["feed"]["daysback"] = daysback
    data["temp"] = temp
    data["log"] = log
    data["output"] = output
    data["output3m"] = output3m
    data["output1y"] = output1y
    data["output3y"] = output3y
    data["outputfl"] = outputfl
    with open('config.json', 'w') as config:
        json.dump(data, config)