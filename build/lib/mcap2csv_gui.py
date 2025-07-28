import mcap2csv
import sys
from PySide6.QtWidgets import QApplication, QMainWindow, QLineEdit, QPushButton, QWidget, QGridLayout, QLabel, QFileDialog

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("mcap2csv GUI")
        self.setGeometry(100, 100, 500, 150)

        self.mcap_file_input = Lineedit_allow_drag()
        self.mcap_file_input.setPlaceholderText("/path/to/your_rosbag.mcap")
        self.mcap_file_input.setToolTip("Enter the path to the MCAP file you want to convert.")

        self.output_path_input = Lineedit_allow_drag()
        self.output_path_input.setPlaceholderText("/path/to/output_dir")
        self.output_path_input.setToolTip("If not provided, defaults to '/path/to/your_rosbag_dir/csv'.")

        self.button = QPushButton("convert")
        self.button.clicked.connect(self.on_button_click)

        self.mcap_file_input_button = QPushButton("Browse")
        self.mcap_file_input_button.clicked.connect(self.on_button_filename)

        self.output_path_input_button = QPushButton("Browse")
        self.output_path_input_button.clicked.connect(self.on_button_output_path)

        label1 = QLabel("MCAP File Path:")
        label2 = QLabel("Output Path (optional):")

        layout = QGridLayout()
        layout.addWidget(label1, 0, 0, 1, 2)
        layout.addWidget(label2, 1, 0, 1, 2)
        layout.addWidget(self.mcap_file_input, 0, 2, 1, 4)
        layout.addWidget(self.output_path_input, 1, 2, 1, 4)
        layout.addWidget(self.mcap_file_input_button, 0, 6)
        layout.addWidget(self.output_path_input_button, 1, 6)
        layout.addWidget(self.button, 2, 2, 1, 2)

        central_widget = QWidget()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

    def on_button_filename(self):
        file_dialog = QFileDialog(self, "Select MCAP File")
        file_dialog.setFileMode(QFileDialog.ExistingFile)
        if file_dialog.exec():
            selected_files = file_dialog.selectedFiles()
            if selected_files:
                self.mcap_file_input.setText(selected_files[0])

    def on_button_output_path(self):
        file_dialog = QFileDialog(self, "Select Output Directory")
        file_dialog.setFileMode(QFileDialog.Directory)
        if file_dialog.exec():
            selected_dirs = file_dialog.selectedFiles()
            if selected_dirs:
                self.output_path_input.setText(selected_dirs[0])

    def on_button_click(self):
        mcap2csv.mcap2csv(
            self.mcap_file_input.text(),
            self.output_path_input.text() or None
        )

# https://touch-sp.hatenablog.com/entry/2024/01/19/185358
class Lineedit_allow_drag(QLineEdit):
    def __init__(self):
        super().__init__()

        self.setDragEnabled(True)

    def dragEnterEvent(self, e):
        if(e.mimeData().hasUrls()):
            e.accept()
    
    def dropEvent(self, e):
        urls = e.mimeData().urls()
        url = urls[0]
        self.setText(url.toLocalFile())

def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()