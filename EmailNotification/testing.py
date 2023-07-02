import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from tableau_api_lib import TableauServerConnection as TSC
from tableau_api_lib.utils import querying, flatten_dict_column

json_file = 'TableauAuth.json'
with open(json_file, 'r') as file:
        credentials = json.load(file)

config = {
        'tableau_online': {
            'server': credentials['server'],
            'api_version': credentials['api_version'],
            'personal_access_token_name': credentials['personal_access_token_name'],
            'personal_access_token_secret': credentials['personal_access_token_secret'],
            'site_name': credentials['site_name'],
            'site_url': credentials['site_url']
        }
    }
conn = TSC(config, env='tableau_online')
conn.sign_in()

views_df = querying.get_views_dataframe(conn)
views_df = flatten_dict_column(views_df, keys=["name", "id"], col_name="workbook")
relevant_views_df = views_df[(views_df["workbook_name"] == "Tufin Paid Marketing") & (views_df["name"] == "Paid Campaign Groups")]
print(relevant_views_df)

"""png_view_id = relevant_views_df["id"].values[0]
response = conn.query_view_image(view_id=png_view_id)
image_data = response.content
image_path = 'C:/Users/esteb/Downloads/image.png'
with open(image_path,'wb') as file:
    file.write(image_data)
print(conn.active_endpoint)
print("Image saved locally at:", image_path)"""