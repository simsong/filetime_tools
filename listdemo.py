import sys
from PyQt5.QtCore import QDate, QSize, Qt
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

# For ideas, see:
# http://nullege.com/codes/show/src@p@y@pyqt5-HEAD@examples@dialogs@configdialog@configdialog.py/220/PyQt5.QtWidgets.QListWidgetItem.setTextAlignment/python

# https://doc.qt.io/qt-5/qlistwidget.html#itemEntered

class VerifyDialog(QDialog):
    def __init__(self, parent=None):
        super(VerifyDialog, self).__init__(parent)

        self.listWidget = QListWidget()

        for i in range(100):
            item = QListWidgetItem("Item %i" % i)
            # could be Qt.Unchecked; setting it makes the check appear
            item.setCheckState(Qt.Checked) 
            self.listWidget.addItem(item)

        runButton = QPushButton("Run")
        runButton.clicked.connect(self.exec)

        cancelButton = QPushButton("Cancel")
        cancelButton.clicked.connect(self.close)
  
        horizontalLayout = QHBoxLayout()
        horizontalLayout.addWidget(self.listWidget, 1)
  
        buttonsLayout = QHBoxLayout()
        buttonsLayout.addStretch(1)
        buttonsLayout.addWidget(runButton)
        buttonsLayout.addWidget(cancelButton)
  
        mainLayout = QVBoxLayout()
        mainLayout.addLayout(horizontalLayout)
        mainLayout.addSpacing(12)
        mainLayout.addLayout(buttonsLayout)
  
        self.setLayout(mainLayout)
        self.setWindowTitle("Config Dialog")
        self.show()

    def exec(self):
        print("The following are selected:")
        allItems = self.listWidget.findItems("",Qt.MatchContains)
        print("Selected items: {}".format(len(allItems)))
        for item in allItems:
            if item.checkState():
                print(item.text())

if __name__=="__main__":
    app = QApplication(sys.argv)
    dialog = VerifyDialog()
    sys.exit(app.exec_())
