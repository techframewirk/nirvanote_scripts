import json
import csv
import numpy as np
import copy
import pdfkit
import decimal
from datetime import datetime
from pathlib import Path
import os 
from tkinter import Tk, filedialog

class InvoiceGen():
  def __init__(self):
    self.output_dir = "./invoice_data"
    self.html_dir = os.path.join(self.output_dir,'html')
    self.pdf_dir = os.path.join(self.output_dir,'pdf')
    if not Path(self.html_dir).exists():
      os.makedirs(self.html_dir)
    if not Path(self.pdf_dir).exists():
      os.makedirs(self.pdf_dir)
    self.added = {}
    self.count = 1
    
    self.get_dir_info()
    self.get_history_of_invoices()
    self.get_invoice_data()
    self.gen_invoice()

  def get_dir_info(self):
    root = Tk()
    root.withdraw()
    print("Select the invoice csv file")
    self.csvfilepath = filedialog.askopenfilename(title="Select the invoice csv file")
    root.update()

  def num2words(self,num):
    num = decimal.Decimal(num)
    decimal_part = num - int(num)
    num = int(num)
    if decimal_part:
        return self.num2words(num) + " point " + (" ".join(self.num2words(i) for i in str(decimal_part)[2:]))

    under_20 = ['Zero', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Eleven', 'Twelve', 'Thirteen', 'Fourteen', 'Fifteen', 'Sixteen', 'Seventeen', 'Eighteen', 'Nineteen']
    tens = ['Twenty', 'Thirty', 'Forty', 'Fifty', 'Sixty', 'Seventy', 'Eighty', 'Ninety']
    above_100 ={ 100: 'Hundred', 1000: 'Thousand', 100000: 'Lakhs', 10000000: 'Crores' }
    if num < 20:
        return under_20[num]
    if num < 100:
        return tens[num // 10 - 2] + ('' if num % 10 == 0 else ' ' + under_20[num % 10])
    pivot = max([key for key in above_100.keys() if key <= num])
    return self.num2words(num // pivot) + ' ' + above_100[pivot] + ('' if num % pivot==0 else ' ' + self.num2words(num % pivot))



  def write_to_csv(self,csv_filepath, data):
      with open(csv_filepath, 'w',newline='') as csvfile: 
          csvwriter = csv.writer(csvfile) 
          csvwriter.writerows(data)

  def read_csv(self,csv_filepath):
      data = csv.reader(open(csv_filepath))
      return list(data)[1:]

  def write_to_json(self,json_filepath, data):
      with open(json_filepath, 'w') as fp:
          json.dump(data, fp, indent=4)

  def get_from_json(self,filepath):
      with open(filepath, 'r') as JSON:
          return json.load(JSON)

  def get_html_string(self):
    html_string = """<!DOCTYPE html>
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta http-equiv="X-UA-Compatible" content="IE=edge" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <link
          rel="stylesheet"
          href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css"
        />
        <style>
          table thead{
            font-weight: normal;
          }
        </style>
        <title>Invoice</title>
      </head>
      <body class="container my-3 border">
        <h1 class="text-center">Tax Invoice</h1>
        <h6 class="mb-5">Billing party</h6>
        <table class="table table-bordered">
          <tbody>
            <thead>
              <th>Invoice no</th>
              <th>invoice_no</th>
              <th>Bank A/c no</th>
              <th>bank_account_number</th>
            </thead>
            <tr>
              <td>Invoice date</td>
              <td>invoice_date</td>
              <td>Bank name</td>
              <td>bank_name</td>
            </tr>
            <tr>
              <td>Name</td>
              <td>beneficiary_name</td>
              <td>Bank IFSC</td>
              <td>bank_ifsc_code</td>
            </tr>
            <tr>
              <td>PAN</td>
              <td>pan_number</td>
              <td>Account type</td>
              <td>account_type</td>
            </tr>
            <tr>
              <td>GST</td>
              <td>gst_number</td>
              <td>UPI ID</td>
              <td>upi_id</td>
            </tr>
          </tbody>
        </table>
        <h6 class="mb-5">Bill to party</h6>
        <table class="table table-bordered">
          <tbody>
            <thead>
              <th>Name:</th>
              <th>
                I-Hub for Robotics and Autonomous Systems Innovation Foundation.
              </th>
            </thead>
            <tr>
              <td>Address:</td>
              <td>
                Society for Innovation and Development, Innovation Centre Building, Indian Institute Science(IISc), Bangalore, 560012.
              </td>
            </tr>
            <tr>
              <td>GSTIN:</td>
              <td>29AAFCI8082A1ZJ</td>
            </tr>
            <tr>
              <td>State:</td>
              <td>Karnataka</td>
            </tr>
          </tbody>
        </table>
        <table class="table table-bordered">
          <tbody>
            <thead>
              <th style="width: 5%;">1</th>
              <th style="width: 85%;">Data quality checks</th>
              <th style="width: 10%;">total_amount</th>
            </thead>
            <tr>
              <td></td>
              <td></td>
              <td></td>
            </tr>
          </tbody>
        </table>
        <table class="table table-bordered">
          <tbody>
            <thead>
              <th style="width: 90%;">amount_in_words</th>
              <th style="width: 10%;">total_amount</th>
            </thead>
          </tbody>
        </table>
        <div class="row">
        
          <div>
            <table class="table table-bordered">
              <tbody>
                <thead>
                  <th style="width: 90%;">Add: GST@18% (if applicable)</th>
                  <th style="width: 10%;">0</th>
                </thead>
                <tr>
                  <td>Deduct: TDS at 10%</td>
                  <td>0</td>
                </tr>
                <tr>
                  <td>Total Amount after TDS</td>
                  <td>total_amount</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <p>I certify that the particulars given above are true and correct</p>
      </body>
    </html>
    """
    return html_string

  def get_history_of_invoices(self):
    self.added = {}
    if Path(os.path.join(self.output_dir,"added.json")).exists():
      self.added = self.get_from_json(os.path.join(self.output_dir,"added.json"))
      del self.added["modified"]
      invoices_nos = self.added.values()
      invoice_suffix_nos = []
      for invoice_no in invoices_nos:
        invoice_suffix_nos.append(int(invoice_no.replace("BI","")))
      self.count =  max(invoice_suffix_nos) + 1

  def get_invoice_data(self):
    data = self.read_csv(self.csvfilepath)
    self.invoice_data = {}
    for row in data:
      invoice_no = "BI" + str(self.count)
      [beneficiary_name,	pan_number,	bank_account_no,	bank_name,	bank_ifsc_code,	account_type,	upi_id,	total_amount] = row
      total_amount = int(total_amount)
      id = str(beneficiary_name+bank_account_no+bank_ifsc_code).strip().lower().replace(" ","")

      if invoice_no in self.added.values():
        for i in range(1000000):
          self.count += 1
          invoice_no = "BI" + str(self.count)
          if invoice_no not in self.added.values():
            break
      
      entry = {
        "invoice_no": invoice_no,
        "beneficiary_name":beneficiary_name,
        "pan_number":pan_number,
        "bank_account_no":bank_account_no,
        "bank_name":bank_name,
        "bank_ifsc_code":bank_ifsc_code,
        "account_type":account_type,
        "upi_id":upi_id,
      }
      if id in self.invoice_data.keys():
        self.invoice_data[id]['total_amount'] += total_amount
      else:
        entry['total_amount'] = total_amount
        self.invoice_data[id] = entry
      self.added[id] = invoice_no
      self.count += 1
    self.added["modified"] = str(datetime.today().strftime('%Y-%m-%d %H:%M:%S'))

  def write_to_file(self,filename,string):
      with open(filename, 'w',newline='') as file: 
          file.write(string)

  def gen_invoice(self):
    csv_data = []
    for keys,single_data in self.invoice_data.items():
        invoice_no = str(single_data["invoice_no"])
        beneficiary_name = str(single_data['beneficiary_name'])
        pan_number = str(single_data['pan_number'])
        bank_account_no = str(single_data['bank_account_no'])
        bank_name = str(single_data['bank_name'])
        bank_ifsc_code = str(single_data['bank_ifsc_code'])
        account_type = str(single_data['account_type'])
        upi_id = str(single_data['upi_id'])
        total_amount = str(single_data['total_amount'])
        amount_in_words = self.num2words(int(total_amount)) + " rupees only"
        invoice_date = datetime.today().strftime('%Y-%m-%d')

        template = copy.copy(self.get_html_string())
        id = beneficiary_name + bank_account_no + bank_ifsc_code
        gst_number = ""

        template = template.replace("invoice_no",invoice_no)
        template = template.replace("invoice_date",invoice_date)
        template = template.replace("beneficiary_name",beneficiary_name)
        template = template.replace("bank_account_number",bank_account_no)
        template = template.replace("invoicedate",invoice_date)
        template = template.replace("bank_name",bank_name)
        template = template.replace("bank_ifsc_code",bank_ifsc_code)
        template = template.replace("gst_number",gst_number)
        template = template.replace("account_type",account_type)
        template = template.replace("upi_id",upi_id)
        template = template.replace("total_amount",total_amount)
        template = template.replace("amount_in_words",amount_in_words)
        template = template.replace("pan_number",pan_number)

        html_filepath = os.path.join(self.html_dir,beneficiary_name+" "+bank_account_no+" "+bank_name+".html")
        self.write_to_file(html_filepath,template)
        pdf_filepath = os.path.join(self.pdf_dir,beneficiary_name+" "+bank_account_no+" "+bank_name+".pdf")
        pdfkit.from_file(html_filepath,pdf_filepath)

        csv_data.append([invoice_no,bank_name, bank_ifsc_code, bank_account_no, beneficiary_name, total_amount])
    
    self.write_to_json(os.path.join(self.output_dir,"added.json"),self.added)
    self.write_to_json(os.path.join(self.output_dir,"invoice.json"),self.invoice_data)
    self.write_to_csv(os.path.join(self.output_dir,"summary.csv"),csv_data)

InvoiceGen()