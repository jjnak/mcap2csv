#!/usr/bin/env python3

# install necessary packages
# pip install mcap-ros2-support numpy pandas

# usage
# python mcap2csv.py /path/to/your_rosbag.mcap /path/to/output_folder

import os
import sys
import pandas as pd
import argparse
import logging
from mcap_ros2.reader import read_ros2_messages

# Configure logging for better feedback
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# Function to flatten ROS messages, including all nested numeric, string, and boolean fields
def flatten_ros_message(msg, prefix=""):
    """
    Flattens a ROS message into a dictionary, extracting basic types (numeric, string, boolean)
    and recursively flattening nested messages.
    """
    flattened_ros_msg = {}  # empty dict

    # check if msg object has __slots__ like header, name, position etc.
    if hasattr(msg, "__slots__"):

        for field_name in msg.__slots__:
            value = getattr(msg, field_name)

            if prefix:
                # add filed_name to prefix, e.g, header.stamp
                key = prefix + "." + field_name
            else:
                key = field_name

            if isinstance(value, (int, float, str, bool)):
                # if value is either int, float, str or blool, add a new pair (key, value) to dict
                flattened_ros_msg[key] = value
            elif isinstance(value, (list, tuple)):
                # if value is either list or tuple
                # example
                # list of float: float32[] (array of float32) [1.2, 2.3, 4.0]
                # list of Point: geometry_msgs/Point[] points [Point(x=1,y=2,z=3), Point(x=3,y=5,z=6)]
                for i, v in enumerate(value):  # i: index, v: value of a list or tuple
                    # Handle lists of basic types or nested messages
                    if isinstance(v, (int, float, str, bool)):
                        flattened_ros_msg[f"{key}[{i}]"] = v  # add index [0], [1], [2]
                    elif hasattr(v, "__slots__"):
                        flattened_ros_msg.update(flatten_ros_message(v, prefix=f"{key}[{i}]"))
                    else:
                        logging.debug(
                            f"Skipping unsupported list element type for {key}[{i}]: {type(v)}"
                        )
            elif hasattr(value, "__slots__"):
                # Recursively flatten nested ROS messages
                flattened_ros_msg.update(flatten_ros_message(value, prefix=key))
            else:
                logging.debug(
                    f"Skipping unsupported field type for {key}: {type(value)}"
                )
    elif isinstance(msg, (int, float, str, bool)):
        # Handle cases where the message itself is a basic type (e.g., std_msgs/Float64)
        flattened_ros_msg[prefix] = msg

    return flattened_ros_msg


def main():
    """
    Reads a ROS2 MCAP bag file, flattens messages, and saves them to
    separate CSV files per topic. A 'timestamp' column is always added
    using `msg_entry.log_time` in seconds, and any internal timestamps
    (like `header.stamp` or `rcl_interfaces/msg/Log.stamp`) are also
    preserved as separate columns. Note log_time seems to be given 
    in local time zone (e.g., JST), but header.stamp is given in UTC.
    """
    parser = argparse.ArgumentParser(description="Convert ROS2 MCAP bag to CSV")
    parser.add_argument("mcap_file", help="Path to the MCAP bag file")
    parser.add_argument(
        "--output_path", help="Directory where the CSV files will be saved"
    )
    args = parser.parse_args()


    mcap_file = os.path.abspath(args.mcap_file)
    output_path = os.path.abspath(args.output_path) if args.output_path else None

    # Validate MCAP file path
    if not os.path.exists(mcap_file):
        logging.error(f"Error: File not found: {mcap_file}")
        sys.exit(1)

    # if output_path is not provided
    if output_path is None:
        output_path = os.path.dirname(mcap_file) + '/csv'  
        logging.info(f"No output path provided. Defaulting to: {output_path}")

    # Create output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logging.info(f"Created output directory: {output_path}")

    topic_data = {}
    message_counter = 0

    logging.info(f"Reading MCAP bag: {mcap_file}")

    try:
        # Iterate through messages in the MCAP file
        for msg_entry in read_ros2_messages(mcap_file):
            message_counter += 1
            if message_counter % 1000 == 0:
                print(f"\rProcessing message {message_counter}...", end="", flush=True)

            topic = msg_entry.channel.topic
            ros_msg = msg_entry.ros_msg

            if ros_msg is None:
                logging.warning(
                    f"Skipping message from topic '{topic}' due to deserialization failure."
                )
                continue

            # 1. Flatten the ROS message payload to get all its fields, including any internal timestamps
            flat_msg = flatten_ros_message(ros_msg)

            # 2. ALWAYS add the log_time as the primary 'log_timestamp' column in nanoseconds initially
            # This ensures every row gets a timestamp from the bag's recording perspective.
            flat_msg["log_timestamp_ns"] = msg_entry.log_time

            if not flat_msg:
                # This condition should now rarely, if ever, be met for valid messages
                # because 'log_timestamp_ns' is always added.
                logging.debug(
                    f"Skipping message from topic '{topic}' as no extractable fields were found (even after adding log_timestamp)."
                )
                continue

            # Store flattened message data per topic
            if topic not in topic_data:
                topic_data[topic] = []
            topic_data[topic].append(flat_msg)

        print("\nFinished reading MCAP bag.")

    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the MCAP file: {e}")
        sys.exit(1)

    # Handle case where no data was extracted
    if not topic_data:
        logging.warning(
            "No messages found or no suitable data could be extracted for CSV conversion."
        )
        sys.exit(0)  # Exit with 0 as it's not an error condition if no data is present

    # Write each topic's data to a separate CSV file
    for topic, messages in topic_data.items():
        # Create a Pandas DataFrame from the list of flattened messages
        df = pd.DataFrame(messages)

        # Process timestamp column (now guaranteed to exist from log_time)
        if "log_timestamp_ns" in df.columns:
            # Ensure 'log_timestamp_ns' is numeric (e.g., int64) to prevent TypeError during division
            df["log_timestamp_ns"] = pd.to_numeric(
                df["log_timestamp_ns"], errors="coerce"
            )

            # Convert nanoseconds timestamp to seconds (float)
            df["log_timestamp"] = df["log_timestamp_ns"] / 1_000_000_000.0

            # Reorder columns: put 'timestamp' at the very beginning, then original flattened fields, then original 'header.stamp.sec' and 'header.stamp.nanosec' if they exist.

            # Get all columns except the ones we want to reorder or drop
            # other_cols = [col for col in df.columns if col not in ['log_timestamp', 'log_timestamp_ns']]
            other_cols = []  # 1. Start with an empty list
            for col in df.columns:  # 2. Loop through each column name
                # 3. Check the condition for each column name
                if col not in ["log_timestamp", "log_timestamp_ns"]:
                    other_cols.append(
                        col
                    )  # 4. If the condition is met, add to the list

            # Columns in desired order: 'log_timestamp' first, then 'header.stamp.sec', 'header.stamp.nanosec', then everything else.
            # Check if header.stamp.sec/nanosec actually exist before trying to order them.
            ordered_cols = ["log_timestamp"]
            if "header.stamp.sec" in other_cols:
                ordered_cols.append("header.stamp.sec")
                other_cols.remove("header.stamp.sec")
            if "header.stamp.nanosec" in other_cols:
                ordered_cols.append("header.stamp.nanosec")
                other_cols.remove("header.stamp.nanosec")

            # For /rosout specifically, it will have 'stamp.sec' and 'stamp.nanosec'
            if (
                "stamp.sec" in other_cols and topic == "/rosout"
            ):  # only apply for /rosout
                ordered_cols.append("stamp.sec")
                other_cols.remove("stamp.sec")
            if (
                "stamp.nanosec" in other_cols and topic == "/rosout"
            ):  # only apply for /rosout
                ordered_cols.append("stamp.nanosec")
                other_cols.remove("stamp.nanosec")

            # Add the remaining columns
            ordered_cols.extend(other_cols)

            # redefine df with re-ordered columns
            df = df[ordered_cols]

            # Remove the raw nanosecond timestamp column used for conversion
            if "log_timestamp_ns" in df.columns:
                df = df.drop(columns=["log_timestamp_ns"])
        else:
            logging.warning(
                f"Topic '{topic}' DataFrame does not contain 'log_timestamp_ns' column. Timestamp processing skipped for this topic."
            )

        # Sanitize topic name for use as a filename
        csv_filename_from_topic_name = topic.replace("/", "_").strip("_")
        csv_file = os.path.join(output_path, csv_filename_from_topic_name + ".csv")

        # Save DataFrame to CSV
        df.to_csv(csv_file, index=False)
        logging.info(
            f"Saved {len(messages)} messages from topic '{topic}' to {csv_file}"
        )


if __name__ == "__main__":
    main()
