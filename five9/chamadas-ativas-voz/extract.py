# -*- coding: utf-8 -*-
def getDataAPI(url, headers, folderName, reportName, date, dayMin=0, dayMax=0):

    import requests, xmltodict, time
    from datetime import timedelta

 
    def parseText(response):

        return xmltodict.parse(response.text)


    def getDataReportResultCsv(response):

        return parseText(response)['env:Envelope']['env:Body']['ns2:getReportResultCsvResponse']['return']


    def getReportResultCsv(id):
        payload = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:getReportResultCsv>
                        <identifier>{id}</identifier>
                    </ser:getReportResultCsv>
                </soapenv:Body>
            </soapenv:Envelope>
        """

        return getDataReportResultCsv(requests.post(url=url, headers=headers, data=payload))


    def getResultReportRunning(response):

        return parseText(response)['env:Envelope']['env:Body']['ns2:isReportRunningResponse']['return']


    def isReportRunning(id):

        payload = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:isReportRunning>
                        <identifier>{id}</identifier>
                    </ser:isReportRunning>
                </soapenv:Body>
            </soapenv:Envelope>
        """
        
        return getResultReportRunning(requests.post(url=url, headers=headers, data=payload))


    def getIdentifier(response):

        return parseText(response)['env:Envelope']['env:Body']['ns2:runReportResponse']['return']


    def runReport():

        startTime = str(date - timedelta(dayMin)) + "T00:00:00.000-03:00"
        endTime = str(date - timedelta(dayMax)) + "T23:59:59.000-03:00"

        payload = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.admin.ws.five9.com/">;
                <soapenv:Header/>
                <soapenv:Body>
                    <ser:runReport>
                        <folderName>{folderName}</folderName>
                        <reportName>{reportName}</reportName>
                        <criteria>
                            <time>
                                <end>{endTime}</end>
                                <start>{startTime}</start>
                            </time>
                        </criteria>
                    </ser:runReport>
                </soapenv:Body>
            </soapenv:Envelope>"""

        return getIdentifier(requests.post(url=url, headers=headers, data=payload))


    try:
        identifier = runReport()
    except Exception as e:
        print(f"Error to run report from API: {e}")
        exit(1)

    try:
        isRunning = isReportRunning(id=identifier)
    except Exception as e:
        print(f"Error to test method isReportRunning: {e}")
        exit(1)

    while isRunning != 'false':
        print("Report is running. Wait.")
        time.sleep(15)

        try:
            isRunning = isReportRunning(id=identifier)
        except Exception as e:
            print(f"Error to test method isReportRunning: {e}")
            exit(1)


    return getReportResultCsv(id=identifier)