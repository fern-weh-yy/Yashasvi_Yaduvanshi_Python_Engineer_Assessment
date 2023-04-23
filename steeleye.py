import requests
import logging
import xml.etree.ElementTree as ET
import io
import zipfile
import csv
import pandas as pd

logging.basicConfig(filename='logger.log', level=logging.INFO)

class DataExtractor:
    def __init__(self, url):
        self.url = url
        self.download_link = None
        self.xml_content = None
        self.data = []

    def download_xml(self):
        """
        Downloads the XML content from the provided URL.
        """
        response = requests.get(self.url)

        if response.status_code == 200:
            self.xml_content = response.content
            logging.info('XML file downloaded successfully.')
        else:
            logging.error('Error in downloading XML file.')

    def find_download_link(self):
        """
        Finds the download link for the DLTINS file type in the XML content.
        """
        root = ET.fromstring(self.xml_content)

        for result in root.findall('.//result'):
            for doc in result.findall('./doc'):
                file_type = doc.find('./str[@name="file_type"]').text
                if file_type == 'DLTINS':
                    self.download_link = doc.find('./str[@name="download_link"]').text
                    logging.info('Download link found')
                    break

        if self.download_link is None:
            logging.error('No download link found')

    def download_zip_and_extract_xml(self):
        """
        Downloads the ZIP file and extracts the XML content.
        """
        response = requests.get(self.download_link)

        if response.status_code == 200:
            zip_content = response.content
            logging.info('Zip Downloaded')
        else:
            logging.error('Error in downloading ZIP file')

        try:
            zip_data = io.BytesIO(zip_content)

            with zipfile.ZipFile(zip_data, 'r') as zip_file:
                xml_filename = zip_file.namelist()[0]
                self.xml_content = zip_file.read(xml_filename)

            with open('input.xml', 'w') as file:
                file.write(self.xml_content.decode())

        except:
            logging.info('Failed to extract XML from ZIP')

    def parse_xml_and_create_dataframe(self):
        """
        Parses the XML content, extracts the required data, and creates a DataFrame.
        """
        tree = ET.parse('input.xml')
        root = tree.getroot()
        cols = ['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                'FinInstrmGnlAttrbts.NtnlCcy', 'Issr']
        parentTags = ['TermntdRcrd', 'ModfdRcrd', 'NewRcrd']

        for tags in root.iter():
            if tags.tag.split("}")[1] in parentTags:
                row = []
                flag1 = flag2 = False
                for FirstF in tags:
                    if 'FinInstrmGnlAttrbts' in FirstF.tag:
                        flag1 = True
                        for values in FirstF:
                            if 'FinInstrmGnlAttrbts.' + values.tag.split("}")[1] in cols:
                                row.append(values.text)
                    elif cols[5] in FirstF.tag:
                        flag2 = True
                        row.append(FirstF.text)
                if flag1 and flag2:
                    self.data.append(row)

        df = pd.DataFrame(self.data, columns=cols)
        df.to_csv('output.csv')

if __name__ == '__main__':
    url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'
    
    extractor = DataExtractor(url)
    extractor.download_xml()
    extractor.find_download_link()
    extractor.download_zip_and_extract_xml()
    extractor.parse_xml_and_create_dataframe()
    logging.info('Data extraction and processing completed. Output saved to output.csv.')