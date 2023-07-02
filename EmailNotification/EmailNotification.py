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

def authenticate_gmail():
    try:
        with open('GmailCredentials.json') as file:
            gmail_credentials = json.load(file)
            gmail_email = gmail_credentials['email']
            gmail_password = gmail_credentials['password']
            return gmail_email, gmail_password
    except FileNotFoundError:
        print("Gmail credentials JSON file not found.")
    except Exception as e:
        print(f"An error occurred during Gmail authentication: {str(e)}")
    return None, None

def authenticate_google_sheets(credentials_file):
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        print("Google Sheets authentication successful!")
        return client
    except FileNotFoundError:
        print("Google Sheets credentials JSON file not found.")
    except Exception as e:
        print(f"An error occurred during Google Sheets authentication: {str(e)}")
    return None

def open_google_sheet(client, spreadsheet_url):
    try:
        spreadsheet = client.open_by_url(spreadsheet_url)
        print("Google Sheet opened successfully!")
        return spreadsheet
    except gspread.exceptions.APIError as e:
        print(f"Error opening Google Sheet: {e.response}")
    except Exception as e:
        print(f"An error occurred while opening Google Sheet: {e}")
    return None

def get_current_datetime():
    current_date = datetime.datetime.now().date()
    current_time = datetime.datetime.now().time()
    return current_date, current_time

def process_email_data(row, current_date, current_time, email_content_tab, conn, gmail_email, gmail_password, html_bodies, images, recipients_list, subject_list):
    email_id, company, vcp, meeting_name, subject, recipients, send_date_str, send_time_str = row[:8]
    send_date = datetime.datetime.strptime(send_date_str, "%d/%m/%Y").date()
    send_time = datetime.datetime.strptime(send_time_str, "%H:%M").time()

    if send_date == current_date and send_time <= current_time:
        print(f"Processing email for {email_id}...")

        email_content_data = email_content_tab.get_all_values()
        content_header = email_content_data[0]
        content_data = email_content_data[1:]

        html_body = f"""
        <html>
            <body>
                <h2>Hi,</h2>
                <p>Please find the screenshot images from the following Tableau workbooks attached:</p>
        """

        tableau_links = []
        for content_row in content_data:
            content_email_id, content_title, tableau_link, workbook_name, view_name = content_row[:5]

            if content_email_id == email_id:
                image_data = get_tableau_image(conn, workbook_name, view_name)
                if image_data:
                    images.append((f'{content_title}.png', image_data))
                    tableau_links.append(f"{view_name} {tableau_link}")

        if images:
            html_body += "<ul>"
            for link in tableau_links:
                html_body += f"<li>{link}</li>"
            html_body += "</ul>"
            html_body += "</body></html>"

            html_bodies.append(html_body)
            recipients_list.append(recipients)
            subject_list.append(subject)

            print(f"Email processed for {email_id}")

def get_tableau_image(conn, workbook_name, view_name):
    views_df = querying.get_views_dataframe(conn)
    views_df = flatten_dict_column(views_df, keys=["name", "id"], col_name="workbook")

    relevant_views_df = views_df[(views_df["workbook_name"] == workbook_name) & (views_df["name"] == view_name)]
    if not relevant_views_df.empty:
        png_view_id = relevant_views_df["id"].values[0]
        response = conn.query_view_image(view_id=png_view_id)
        print(response)
        image_data = response.content
        return image_data
    return None

def send_email(sender_email, sender_password, recipients, subject, html_bodies, images):
    for i in range(len(html_bodies)):
        html_body = html_bodies[i]
        recipients = recipients[i]
        subject = subject[i]
        attachment_filenames = []
        attachment_data = []

        for image in images:
            attachment_filenames.append(image[0])
            attachment_data.append(image[1])

        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = recipients
        message["Subject"] = subject

        message.attach(MIMEText(html_body, "html"))

        for j in range(len(attachment_filenames)):
            attachment = MIMEImage(attachment_data[j])
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_filenames[j])
            message.attach(attachment)

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.ehlo()
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipients.split(","), message.as_string())

        print(f"Email sent to {recipients} with subject '{subject}'")

def main():
    gmail_email, gmail_password = authenticate_gmail()
    if not gmail_email or not gmail_password:
        return

    credentials_file = 'turn-river-capital-39952ce29499.json'
    client = authenticate_google_sheets(credentials_file)
    if not client:
        return

    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1LQPu3K0DJq8vLBgr3EL0mHrixZ3YEPrC9CYN5mh8g7A'
    spreadsheet = open_google_sheet(client, spreadsheet_url)
    if not spreadsheet:
        return

    email_inputs_tab = spreadsheet.worksheet("Email Inputs")
    email_content_tab = spreadsheet.worksheet("Email Content")

    current_date, current_time = get_current_datetime()

    email_inputs_data = email_inputs_tab.get_all_values()
    header = email_inputs_data[0]
    data = email_inputs_data[1:]

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

    html_bodies = []
    images = []
    recipients_list = []
    subject_list = []

    for row in data:
        process_email_data(row, current_date, current_time, email_content_tab, conn, gmail_email, gmail_password, html_bodies, images, recipients_list, subject_list)

    if images:
        send_email(gmail_email, gmail_password, recipients_list, subject_list, html_bodies, images)

    print("Task completed successfully!")


if __name__ == "__main__":
    main()
