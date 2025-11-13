from common.config import get_connection

from datetime import datetime, date
from decimal import Decimal
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def close_db(cursor, conn):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
def table_exists(conn, table_name, schema='public'):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """, (schema, table_name))
        return cur.fetchone()[0]

# Safely serializes non-JSON-serializable data types (like datetime, Decimal, bytes) 
def safe_serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()  
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, bytes):
        return value.decode('utf-8', errors='ignore')
    return value

# CREATE OPERATIOn
def CREATE(Database_name, Table_name, Values):
    conn = cursor = None
    try:
        conn = get_connection(Database_name)
        cursor = conn.cursor()
        cols = ', '.join(Values.keys())
        placeholders = ', '.join(['%s'] * len(Values))
        cursor.execute(
            f"INSERT INTO {Table_name} ({cols}) VALUES ({placeholders})",
            tuple(Values.values())
        )
        conn.commit()
        return {"message": f"Inserted data into {Table_name} successfully"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(cursor, conn)

# READ OPERATIOn
# def READ(Database_name, Table_name, Conditions=None, Columns="*", OrderBy=None):
def READ(Database_name, Table_name, Conditions, Columns, OrderBy):
    conn = cursor = None
    try:
        conn = get_connection(Database_name)
        cursor = conn.cursor()
        # Columns = []
        query = f"SELECT {Columns} FROM {Table_name}"
        params = []

        if Conditions:
            where = ' AND '.join([f"{k}=%s" for k in Conditions.keys()])
            query += f" WHERE {where}"
            params = list(Conditions.values())

        if OrderBy:
            query += f" ORDER BY {OrderBy}"

        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]

        # Convert all values to JSON-safe types
        serialized_rows = []
        for r in rows:
            serialized_row = {k: safe_serialize(v) for k, v in zip(cols, r)}
            serialized_rows.append(serialized_row)

        return {"data": serialized_rows}

    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(cursor, conn)

# UPDATE OPERATIOn
def UPDATE(Database_name, Table_name, Updates, Conditions):
    conn = cursor = None
    try:
        conn = get_connection(Database_name)
        cursor = conn.cursor()
        set_clause = ', '.join([f"{k}=%s" for k in Updates.keys()])
        where = ' AND '.join([f"{k}=%s" for k in Conditions.keys()])
        cursor.execute(
            f"UPDATE {Table_name} SET {set_clause} WHERE {where}",
            tuple(Updates.values()) + tuple(Conditions.values())
        )
        conn.commit()
        return {"message": f"Updated data in {Table_name} successfully"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(cursor, conn)


# DELETE OPERATIOn
def DELETE(Database_name, Table_name, Conditions):
    conn = cursor = None
    try:
        conn = get_connection(Database_name)
        cursor = conn.cursor()
        where = ' AND '.join([f"{k}=%s" for k in Conditions.keys()])
        cursor.execute(
            f"DELETE FROM {Table_name} WHERE {where}",
            tuple(Conditions.values())
        )
        conn.commit()
        return {"message": f"Deleted record(s) from {Table_name} successfully"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        close_db(cursor, conn)


# Methods 
METHODS = {
    "CREATE": CREATE,
    "READ": READ,
    "UPDATE": UPDATE,
    "DELETE": DELETE
}
# Delete - > truncate 



def send_mail(sender_email, password, to_email, subject, body, attachment_path=None,
              smtp_server="smtp.gmail.com", smtp_port=587):

    # Create email container
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = to_email
    msg["Subject"] = subject

    # Email body
    msg.attach(MIMEText(body, "plain"))

    # Add attachment (optional)
    if attachment_path:
        try:
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f"attachment; filename={attachment_path.split('/')[-1]}")
            msg.attach(part)
        except Exception as e:
            print(f"Error loading attachment: {e}")
            return False

    try:
        # Connect to SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, password)

        # Send email
        server.sendmail(sender_email, to_email, msg.as_string())
        server.quit()

        print("Email sent successfully!")
        return True

    except Exception as e:
        print(f"Error sending email: {e}")
        return False

# send_mail(
#     sender_email="your_email@gmail.com",
#     password="your_app_password",
#     to_email="receiver@gmail.com",
#     subject="Test Email",
#     body="This is a test email.",
#     attachment_path=None  # or "file.pdf"
# )