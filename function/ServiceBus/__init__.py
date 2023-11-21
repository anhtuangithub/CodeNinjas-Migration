import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime

def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    conn = psycopg2.connect(host = "codeninjasv.postgres.database.azure.com",
                            port="5432",
                            user = "codeninja@codeninjasv",
                            password = "matKhau123",
                            dbname = "techconfdb")
    try:
        cursor = conn.cursor()
        # TODO: Get notification message and subject from database using the notification_id
        get_notification_query = "SELECT message, subject FROM notification WHERE ID = %s;"
        cursor.execute(get_notification_query, (notification_id,))
        notification_data = cursor.fetchone()
        # TODO: Get attendees email and name
        get_attendees_query = "SELECT COUNT(*) FROM attendee;"
        cursor.execute(get_attendees_query)
        attendee_data = cursor.fetchall()
        attendee_count = len(attendee_data)
        # TODO: Loop through each attendee and send an email with a personalized subject

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        update_notification_query = "UPDATE notification SET completed_date = %s, status = %s WHERE ID = %s;"
        cursor.execute(update_notification_query,(datetime.utcnow(), 'Notified {} attendees'.format(attendee_count), notification_id))

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if 'conn' in locals() and conn is not None:
            cursor.close()
            conn.close()
