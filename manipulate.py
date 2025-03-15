import pandas as pd
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

wt_i_wt = {'Student population':'DRVEF2022.Total enrollment',
           'Average class size (overall)':'DRVEF2022.Undergraduate enrollment',
           'Student-faculty ratio (overall)':'EF2022D.Student-to-faculty ratio',
           'Average admitted SAT/ACT':'SAT',
           'Tuition and fees (per year)':'DRVIC2022.Tuition and fees, 2022-23',
           'Aid/scholarships offered/available':'AID',
           ' rate':'DRVADM2022.Percent admitted - total.1',
           '4-year graduation rate':'DRVGR2022.Graduation rate - Bachelor degree within 4 years, total'}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

class updateCellsRequest():
  def __init__(self, sheetId, startRowIndex, endRowIndex, startColIndex, endColIndex, values, sheetName=0):
    self.sheetId = sheetId
    self.startRowIndex = startRowIndex
    self.endRowIndex = endRowIndex
    self.startColIndex = startColIndex
    self.endColIndex = endColIndex
    self.values = values
    self.fields = "*"
    self.sheetName = sheetName
  def construct(self):
    return {
      "updateCells":
        {
          "range":
            {
            "sheetId": self.sheetName,
            "startRowIndex": int(self.startRowIndex)-1,
            "endRowIndex": int(self.endRowIndex),
            "startColumnIndex": self.startColIndex,
            "endColumnIndex": self.endColIndex+2
            },
            "rows": [
              {
                "values":[{"userEnteredValue": {"stringValue": str(self.values)}}]
              }
            ],
            "fields":"*"
        }
      }


def get_college_data(name:str, df:pd.DataFrame, info:str):
  if info in wt_i_wt:
    key = wt_i_wt[info]
    try:
      college = df.loc[df['institution name'] == name].iloc[0]
    except:
      return None
    if key == 'SAT':
      return "SAT: " + str(int(college['ADM2022.SAT Evidence-Based Reading and Writing 50th percentile score']) + int(college['ADM2022.SAT Math 50th percentile score'])) + " ACT: " + str(college['ADM2022.ACT Composite 50th percentile score'])
    if "rate" in key:
      return str(college[key]) + "%"
    if key == 'AID':
      return f'Average aid: {college["SFA2122.Average amount of federal, state, local or institutional grant aid awarded"]}, Average price: {college["SFA2122.Average net price-students awarded grant or scholarship aid, 2021-22.1"]}'
    return college[key]
  return None


def get_data(sheet, sheet_id, cells):
    result = (
        sheet.values()
        .get(spreadsheetId=sheet_id, range=cells)
        .execute()
    )
    values = result.get("values", [])
    if not values:
      print("No data found.")
      return
    return values

\

def update_sheet(sheet, spreadsheet_id, values:list):
  if values == None or values == [] or type(values) != list:
    print("Not updating")
    return None
  body = {"requests": values}
  print(values[0])
  request = sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body)
  result = request.execute()
  return result
  
  
def main():
  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  dfa = pd.read_csv('AllDataBackup.csv',index_col=False)
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "creds.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build("sheets", "v4", credentials=creds)
    sheet_id = input("Enter the URL to the Sheet  ")
    gid = sheet_id.split("gid=")[-1]
    sheet_id = sheet_id.split('/')[-2]
    print(gid)
    sheet_name = input("Enter sheet name, if using more than 1 sheet ")
    print(sheet_id)
    sheet = service.spreadsheets()
    data_to_fill = get_data(sheet, sheet_id, f'{sheet_name}!A6:A7')
    print(data_to_fill)
    colleges = get_data(sheet, sheet_id,f'{sheet_name}!B1:AZ1')[0]
    c_college = 1
    all_requests = []

    
    for college in colleges:
      values = []
      v_range = list([2, 2])
      for data in data_to_fill:
        if data != []:
          print(data)
          print(f"Getting {data[0]} for {college}")
          data = get_college_data(college, dfa, data[0])
        else:
          data = None
          print(f'Data is {data}')
        if data == None:
          if values != []:
            all_requests.append(updateCellsRequest(sheet_id, v_range[0], v_range[1]-1, c_college, c_college, values, sheetName=gid).construct())
          values = []
          v_range = [v_range[1]+1, v_range[1]+1]
          continue
        values.append(data)
        [all_requests.append(updateCellsRequest(sheet_id, v_range[0], v_range[1], c_college, c_college, i, sheetName=gid).construct()) for i in values]
        v_range = [v_range[1]+1, v_range[1]+1]        
        values = []
      c_college += 1
    update_sheet(sheet, sheet_id, all_requests)
                

    
  except HttpError as err:
    print(err)

dfa = pd.read_csv('AllDataBackup.csv',index_col=False)

if __name__ == "__main__":
  main()
