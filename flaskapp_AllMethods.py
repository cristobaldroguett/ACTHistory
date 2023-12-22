#!/usr/bin/env python3
import os
import time
import json
from flask import Flask, Response, jsonify, request, render_template
from datetime import datetime, timezone, timedelta
#from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk
from opensearchpy import OpenSearch, Search, RequestsHttpConnection, helpers
from collections import OrderedDict
from opensearchpy.helpers import bulk, scan, search

# Environmental Variables
#host = os.getenv("ESHOST")
osdsn = os.getenv("OPENSEARCHDSN")
osuser = os.getenv("OPENSEARCHUSER")
ospass = os.getenv("OPENSEARCHPASS")
HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH']
#Functions
def opensearch_connection():
    if osdsn is not None:
        elements = osdsn.replace('/', '').split(':')
        if elements[0] == "https":
            use_ssl = True
            port = 443
        else:
            use_ssl = False
            port = 80
        host = elements[1]
        if len(elements) > 2:
            port = elements[2]
        # Hopefully it's a drop in replacement for Elastic API, we shall see...
        os_conn = OpenSearch(hosts=[{'host': host, 'port': port}],
                             use_ssl=use_ssl,
                             http_compress=True,
                             http_auth=(osuser, ospass),
                             verify_certs=False,
                             ssl_show_warn=False,
                             ssl_assert_hostname=False,
                             connection_class=RequestsHttpConnection,
                             timeout=120
                             )
        return os_conn

def hourly_scan(client=None, indices="devicestate-*", body={}, size_limit=0):
    response = client.search(index=indices, body=body, scroll='3s')
    return response

def scrolling(client=None, scroll_id="", scroll='1s'):
    resp = client.scroll(scroll_id=scroll_id, scroll=scroll)
    return resp

app = Flask(__name__)

eventa = {} #This global dict will hold parameters if passed from Postman

@app.route('/health')
def testing():
    return "Success", 200

@app.route('/stream', methods=HTTP_METHODS) #Go to /stream to see result of scroll
def generate():
    response_headers = {
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,X-Amz-Security-Token,Authorization,X-Api-Key,X-Requested-With,Accept,Access-Control-Allow-Methods,Access-Control-Allow-Origin,Access-Control-Allow-Headers",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT"
    }

    # Time when we enter the program
    stream_time_before = datetime.now()
    print("Time when we enter stream is " + str(stream_time_before))

    #Get the input fields from the request body
    startTime = request.json['start_date']  # inputParams[0] #String
    endTime = request.json['end_date']  # inputParams[1]
    SIMID = request.json['SIMID']  # ['array']
    RMEQPNUM = request.json['RMEQPNUM']  # ['array']
    contractNum = request.json['ContractNum']  # ['array']
    AccountCustomerNum = request.json['AccountCustomerNum']  # ['array']
    HitsNumber = request.json['NumberofRecords']

    # Sets the amount of scrolls allowed
    scroll_amount = os.getenv("NUMBEROFSCROLLS")
    scroll_amount = int(scroll_amount)

    # This mimics the structure of body from Postman
    event = {
        "start_date": startTime,
        "end_date": endTime,
        "SIMID": SIMID,
        "RMEQPNUM": RMEQPNUM,
        "ContractNum": contractNum,
        "AccountCustomerNum": AccountCustomerNum,
        "NumberofRecords": HitsNumber
    }

    # Handling the case where user does not pass in start and end dates
    if (startTime == "" or endTime == ""):
        return {
            "statusCode": 500,
            "headers": response_headers,
            "error": "Must pass in a start and end date"
        }

    # Handling the case where user does not pass in at least one device paramerter
    if (SIMID['array'][0] == "" and RMEQPNUM['array'][0] == "" and contractNum['array'][0] == "" and
            AccountCustomerNum['array'][0] == ""):
        return {
            "statusCode": 500,
            "headers": response_headers,
            "error": "Must pass in at least one device field"
        }

    if HitsNumber == "":  # If blank I want to search for everything
        HitsNumber = 10000 * scroll_amount

    #### TEST CODE FOR GETTING ALL DATES BETWEEN ST ET ####
    indexlist = []
    endTimedt = datetime.strptime(endTime, '%Y%m%d')
    startTimedt = datetime.strptime(startTime, '%Y%m%d')
    indexlist.append('devicehistory-fromsnapshot-' + startTime)
    # Get the next day
    if endTime != startTime:
        nextTimedt = startTimedt + timedelta(days=1)
        nextTime = nextTimedt.strftime('%Y%m%d')
        indexlist.append('devicehistory-fromsnapshot-' + nextTime)

        # Loop until we catch up
        while nextTimedt != endTimedt:
            nextTimedt = nextTimedt + timedelta(days=1)
            nextTime = nextTimedt.strftime('%Y%m%d')
            indexlist.append('devicehistory-fromsnapshot-' + nextTime)

    ####   Old Date Pattern Code
    # Create datetime pattern for index query
    # x = len(startTime)
    # y = len(endTime)
    # z = ""
    # for x in range(0, len(startTime)):
    #     if x > len(endTime):
    #         break
    #     if startTime[x] == endTime[x]:
    #         z = z + startTime[x]
    # datepattern = z
    # if len(z) < len(startTime):
    #     datepattern = datepattern + "*"
    #####

    # Format the start and end dates to be datetime objects
    startTime = startTime[:4] + "-" + startTime[4:6] + "-" + startTime[6:]
    endTime = endTime[:4] + "-" + endTime[4:6] + "-" + endTime[6:]  # + " 59:59:59"
    startTime_object = datetime.strptime(startTime, "%Y-%m-%d")
    endTime_object = datetime.strptime(endTime, "%Y-%m-%d")  # T%H:%M:%S.%f")
    endTime_object = endTime_object.replace(hour=23)
    endTime_object = endTime_object.replace(minute=59)
    endTime_object = endTime_object.replace(second=59)
    endTime_object = endTime_object.replace(microsecond=999)
    startTime_object = startTime_object.replace(microsecond=1)

    startTime = str(startTime_object)
    endTime = str(endTime_object)
    startTime = startTime[:10] + "T" + startTime[11:23] + "Z"
    startTime = startTime[:23] + "Z"
    endTime = endTime[:10] + "T" + endTime[11:23] + "Z"

    #time.sleep(0.01)
    # startTime = startTime[:10] + "T" + startTime[11:] + "Z"
    # endTime = endTime[:10] + "T" + endTime[11:] + "Z"
    # Connect to OpenSearch

    # Time when we enter the program
    setup_time_before = datetime.now()
    print("Amount of time taken prior to OS connection: " + str(setup_time_before-stream_time_before) + " seconds")
    try:
        opensearch = opensearch_connection()
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": response_headers,
            "error": f"Failed to connect to OpenSearch: {str(e)}"
        }

    # Build the search query for all the fields
    query_string = ""
    field_string = ""
    triggerOR = False
    triggerAND = False

    input = event  # json.loads(event['body'])
    for key in input.keys():  # event.keys is all the parameter names
        if type(input[key]) == dict:
            if (input[key]['array'][0] != ""):
                if triggerAND:
                    query_string = query_string + " AND "
                field_string = "("
                triggerOR = False
                for field in input[key]['array']:
                    if triggerOR:
                        field_string = field_string + " OR "
                    if key == 'SIMID':
                        field = "*" + field
                    field_string = field_string + str(key) + ".value:" + str(field)
                    triggerOR = True
                field_string = field_string + ")"
                query_string = query_string + field_string
                triggerAND = True

    must_string = [
        {
            "query_string": {
                "query": query_string,
                "analyze_wildcard": True
            }
        }
    ]
    # Making the query
    query = {
        "bool": {
            "must": must_string,
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            # "lte": "2023-10-15T19:20:18.000Z", #This would be changed to their startTime plus one min
                            "lte": endTime,
                            "gte": startTime,
                            "format": "strict_date_optional_time"
                        }
                    }
                }
            ],
            "should": [],
            "must_not": []
        }
    }

    # This include list is the fields we want returned from the first query
    includeList = [
        "AccountCustomerNum",
        "AccountCustomerNum.*",
        "SIMID",
        "RMEQPNUM",
        "ContractNum",
        "ChargeAlternatorStatus",
        "ChargeAlternatorStatus.*",
        "AvgACRMSCurrent",
        "AvgACRMSCurrent.*",
        "AvgLineLineACRMSVoltage",
        "AvgLineLineACRMSVoltage.*",
        "GeneratorAvgLineNeutralACRMSVoltage",
        "GeneratorAvgLineNeutralACRMSVoltage.*",
        "BarometricPressure",
        "BarometricPressure.*",
        "Battery",
        "Battery.*",
        "GeneratorCircuitBreakerTripState",
        "GeneratorCircuitBreakerTripState.*",
        "CheckGenset",
        "CheckGenset.*",
        "CumulativeIdleHours",
        "CumulativeIdleHours.*",
        "CommunicationModeStatus",
        "CommunicationModeStatus.*",
        "CompanyCode",
        "CanbusStatus",
        "CanbusStatus.*",
        "EngineCoolantTemperature",
        "EngineCoolantTemperature.*",
        "WheelBasedVehicleSpeed",
        "WheelBasedVehicleSpeed.*",
        "Speed",
        "Speed.*",
        "ControlPanelStatus",
        "ControlPanelStatus.*",
        "AT1SCRCatalystTankLevel",
        "AT1SCRCatalystTankLevel.*",
        "DEFRemaining",
        "DEFRemaining.*",
        "DEFFluidLevel",
        "DEFFluidLevel.*",
        "DischargePressure",
        "DischargePressure.*",
        "DPF1AshLoadPercent",
        "DPF1AshLoadPercent.*",
        "DPFRegenInhibitStatus",
        "DPFRegenInhibitStatus.*",
        "DPFRegenStatus",
        "DPFRegenStatus.*",
        "DPFStatus",
        "DPFStatus.*",
        "DPF1TimeSinceLastActiveRegen",
        "DPF1TimeSinceLastActiveRegen.*",
        "DPF1SootLoadPercent",
        "DPF1SootLoadPercent.*",
        "EngineSpeed",
        "EngineSpeed.*",
        "Estop",
        "Estop.*",
        "EngineStatus",
        "EngineStatus.*",
        "EngineFuelRate",
        "EngineFuelRate.*",
        "FuelLevel",
        "FuelLevel.*",
        "FuelRemaining",
        "FuelRemaining.*",
        "FlowRate",
        "FlowRate.*",
        "AvgACFrequency",
        "AvgACFrequency.*",
        "LastHibernateMode",
        "LastHibernateMode.*",
        "HighCoolantTempAlarm",
        "HighCoolantTempAlarm.*",
        "IgnitionStatus",
        "IgnitionStatus.*",
        "InternalBatteryCharge",
        "InternalBatteryCharge.*",
        "LowBatteryStatus",
        "LowBatteryStatus.*",
        "LowOilPressureAlarm",
        "LowOilPressureAlarm.*",
        "Source",
        "ModbusStatus",
        "ModbusStatus.*",
        "CumulativeEngineHours",
        "CumulativeEngineHours.*",
        "RunTime",
        "RunTime.*",
        "Odometer",
        "Odometer.*",
        "Meter2YN",
        "Meter2YN.*",
        "FanMode",
        "FanMode.*",
        "FlowRate",
        "FlowRate.*",
        "FuelLevel",
        "FuelLevel.*",
        "GeneratorCircuitBreakerTripState",
        "GeneratorCircuitBreakerTripState.*",
        "GeneratorOverallPowerFactor",
        "GeneratorOverallPowerFactor.*",
        "GeneratorTotalApparentPower",
        "GeneratorTotalApparentPower.*",
        "GeneratorTotalPercentKW",
        "GeneratorTotalPercentKW.*",
        "GeneratorTotalReactivePower",
        "GeneratorTotalReactivePower.*",
        "HeatingSetpoint",
        "HeatingSetpoint.*",
        "HighCoolantTempAlarm",
        "HighCoolantTempAlarm.*",
        "IgnitionStatus",
        "IgnitionStatus.*",
        "InternalBatteryCharge",
        "InternalBatteryCharge.*",
        "LastHibernateMode",
        "LastHibernateMode.*",
        "Location",
        "Location.*",
        "LowBatteryStatus",
        "LowBatteryStatus.*",
        "LowCoolantLevel",
        "LowCoolantLevel.*",
        "LowOilPressureAlarm",
        "LowOilPressureAlarm.*",
        "Meter2YN",
        "Meter2YN.*",
        "OvercurrentStatus",
        "OvercurrentStatus.*",
        "OverFrequencyStatus",
        "OverFrequencyStatus.*",
        "EngineOilPressure",
        "EngineOilPressure.*",
        "OverloadStatus",
        "OverloadStatus.*",
        "GeneratorOverallPowerFactor",
        "GeneratorOverallPowerFactor.*",
        "OverVoltageAlarm",
        "OverVoltageAlarm.*",
        "SuctionPressure",
        "SuctionPressure.*",
        "SystemLevel",
        "SystemLevel.*",
        "GeneratorTotalApparentPower",
        "GeneratorTotalApparentPower.*",
        "EngineTotalFuelUsed",
        "EngineTotalFuelUsed.*",
        "GeneratorTotalPercentKW",
        "GeneratorTotalPercentKW.*",
        "TotalRealPower",
        "TotalRealPower.*",
        "GeneratorTotalReactivePower",
        "GeneratorTotalReactivePower.*",
        "UtilizationTime",
        "UtilizationTime.*",
        "UnderFrequencyStatus",
        "UnderFrequencyStatus.*",
        "UnderVoltageAlarm",
        "UnderVoltageAlarm.*",
        "VendorEqpNum",
        "SrcOffset",
        "SrcOffset.*",
        "VendorEqpNum.*",
        "name",
        "name.*",
        "PingTime"
    ]
    #includeList = ["*"]

    # Format HitsNumber
    HitsNumber = int(HitsNumber)  # Do this earlier in code and same with any other typecast
    if HitsNumber > 10000:
        size = HitsNumber // scroll_amount
    else:
        size = HitsNumber  # This dictates the size of the scroll

    # We only want one record and we sort by ascending timestamp
    querybody = {
        "from": 0,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "_source": {
            "include": includeList
        },
        "query": query,
        "size": size
    }
    indexes = ""
    for index in indexlist:
        # Creating big string for all indexes
        if indexes != "":
            indexes += "," + index
        else:
            indexes += index
    # We build this index pattern to cut down number of indexes to search through

    # Time when we enter the program
    result_time_before = datetime.now()
    print("Amount of time taken prior to OS query: " + str(result_time_before-setup_time_before) + " seconds")
    # This is where we get the result of the aggregation based query
    result = hourly_scan(client=opensearch, indices=indexes, body=querybody)
    # Time when we enter the program
    result_time_after = datetime.now()
    print("Amount of time taken during OS query: " + str(result_time_after - result_time_before) + " seconds")
    # If there is no aggregations bucket, then there were no results
    try:
        result['hits']['hits'][0]
    except Exception as e:
        print('There were no results for those parameters')
        return {
            "statusCode": 500,
            "headers": response_headers,
            "error": f"There were no results for those parameters: {str(e)}"
        }

    displayList = []
    ids_list = []
    total_scrolls = 0
    # If this is true then we need to scroll in a loop, otherwise just do the one query
    if HitsNumber > 10000:
        while total_scrolls < scroll_amount:
            before_scroll = datetime.now()
            old_scroll_id = result['_scroll_id']
            for record in result['hits']['hits']:
                if record['_id'] not in ids_list:
                    displayList.append(record['_source'])
                    ids_list.append(record['_id'])
            total_scrolls += 1
            if total_scrolls == scroll_amount:
                break
            result = scrolling(opensearch, old_scroll_id, "1m")
            scroll_time = datetime.now()
            print("Amount of time taken during scroll: " + str(scroll_time - before_scroll) + " seconds")

    else:
        for record in result['hits']['hits']:
            if record['_id'] not in ids_list:
                displayList.append(record['_source'])
                ids_list.append(record['_id'])
    ### Format the output ###
    output_before = datetime.now()
    new_result = []
    for hit in displayList:
        # return str(hit.keys())
        if 'SIMID' not in hit.keys():
            simid = "NO SIM ID"
        else:
            simid = str(hit['SIMID']['value'])  # Holds the SIMID of the current hit dict
        if 'SIMID' in hit.keys():
            del hit['SIMID']
        new_dict = {"SIMID": simid}
        new_dict.update(hit)
        new_result.append(new_dict)
    #return json.dumps(new_result)
    # Must reverse list so returned in descending order
    reversedlist = list(reversed(new_result))

    # Now that we have the full output, we may need to output however many the user specified
    if HitsNumber == "":
        reversedlist = reversedlist
    elif len(new_result) > int(HitsNumber):
        reversedlist = reversedlist[:int(HitsNumber)]

    ### Output total time ###
    stream_time_after = datetime.now()
    output_after = datetime.now()
    print("Amount of time taken to format output: " + str(output_after - output_before) + " seconds")
    print('Total time for stream is : ' + str(stream_time_after - stream_time_before))
    print('We have successfully sent the data')
    print(str(type(reversedlist)))
    teststr = json.dumps(reversedlist)

    # Output the records
    return Response(teststr, mimetype='application/json')

    # prettyForm = json.dumps(reversedlist, indent=4)
    # return prettyForm


if __name__ == '__main__':
    app.run(host = '0.0.0.0',port=7000) #Change to 7000 for local run
    if os.getenv('DEBUG') is not None:
        app.debug = True