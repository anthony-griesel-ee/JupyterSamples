import sys
import xml.etree.cElementTree as ET
import datetime

def datetime2ole(inputdate: datetime) -> float:
    OLE_TIME_ZERO = datetime.datetime(1899, 12, 30)
    delta = inputdate - OLE_TIME_ZERO
    return float(delta.days) + (float(delta.seconds) / 86400)  # 86,400 seconds in day

def write_xml(modelname: str, date: datetime) -> None:
    oledate = datetime2ole(date)
    root = ET.Element("UndocumentedParam", xmlns="http://tempuri.org/UndocumentedParam.xsd")

    print(f"Setting Date From to {oledate}")
    a = ET.SubElement(root, "Attributes")
    ET.SubElement(a, "Class").text = "Horizon"
    ET.SubElement(a, "Name").text = f"{modelname}"
    ET.SubElement(a, "Attribute").text = "Date From"
    ET.SubElement(a, "Value").text = f"{oledate}"

    print(f"Setting Chrono Date From to {oledate}")
    b = ET.SubElement(root, "Attributes")
    ET.SubElement(b, "Class").text = "Horizon"
    ET.SubElement(b, "Name").text = f"{modelname}"
    ET.SubElement(b, "Attribute").text = "Chrono Date From"
    ET.SubElement(b, "Value").text = f"{oledate}"

    tree = ET.ElementTree(root)
    tree.write("filename.xml")

if __name__ == "__main__":
    try:
        if len(sys.argv) > 2:
            modelstr = sys.argv[1]
            datestr = sys.argv[2]
            datevalue = datetime.datetime.today()
            
            if datestr != 'TODAY':
                datevalue = datetime.strptime(datestr, '%m/%d/%y')
            
            print(f"User specified parameters: MODEL: {modelstr}, DATE: {datevalue}")
            write_xml(modelstr, datevalue)
        else:
            print("Required parameters missing! MODELNAME and DATE required: upatehorizon.py MODELNAME 12/31/24")
    except ValueError as e: 
        print('Configuring Undocmented Parameter dates failed:')
        print(e)