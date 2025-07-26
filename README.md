# mcap2csv: a Python Script to Convert ROS2 Bag (MCAP) Data into CSV
**mcap2csv** — a tool to convert ROS2 bag data in the MCAP file format into CSV.


## Quick Installation

Install mcap2csv module with its dependencies. Using a virtual environment (`python>=3.8`) is recommended.
```bash
pip install git+https://github.com/jjnak/mcap2csv.git
```
For source installation see below.

## Usage
### Prerequisites (Recording bag in MCAP format)

#### Install rosbag2_storage_mcap storage plugin
For ROS2 Humble, install the `rosbag2_storage_mcap` storage plugin. Note that for ROS2 versions newer than Jazzy, the MCAP file format is the default. 

```bash
sudo apt-get install ros-$ROS_DISTRO-rosbag2-storage-mcap
```

#### Record ROS topics in MCAP format
Example: use the `-s mcap` option to record in the MCAP format (note: this option is needed for ROS2 Hubmle).
```bash
ros2 bag record -s mcap --all
```

### Convert the bag data into CSV

```bash
mcap2csv /path/to/your_rosbag.mcap [/path/to/output_dir]
```
* If output_dir is not provided, `/path/to/your_rosbag_dir/csv` directory will be created by default. 

* After conversion, a CSV file for each topic will be generated.

## Notes on Timestamps in the CSV file
* `log_timestamp` is the time when the message is recorded (log_time) in the local time zone of the operating system. This is added for debugging purposes if necessary.

* `header.stamp.sec` and `header.stamp.nanosec` represent the timestamps when the message was generated in UTC.

## Source Installation

1. Install the necessary Python packages:
```bash
pip install mcap-ros2-support numpy pandas
```

2. Clone this mcap2csv repository:
```bash
git clone https://github.com/jjnak/mcap2csv.git
```
3. Navigate into the cloned repository:
```bash
cd mcap2csv
```

4. Add the execute permission to the Python script:
```bash
chmod +x mcap2csv.py
```

## Authors
Jun Nakanishi - Meijo University, Japan

[Shunki Itadera](https://itadera.github.io/) - AIST, Japan 
