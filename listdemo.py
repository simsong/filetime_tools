import sys
from PyQt5.QtCore import QDate, QSize, Qt
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

# For ideas, see:
# http://nullege.com/codes/show/src@p@y@pyqt5-HEAD@examples@dialogs@configdialog@configdialog.py/220/PyQt5.QtWidgets.QListWidgetItem.setTextAlignment/python

# https://doc.qt.io/qt-5/qlistwidget.html#itemEntered

class VerifyDialog(QDialog):
    def saveClickState(self, index):
        item = self.listWidget.currentItem()
        self.newState = Qt.Checked if item.checkState()==Qt.Unchecked else Qt.Unchecked
        item.setCheckState(self.newState)

    def setClickState(self, item):
        item.setCheckState(self.newState)
        
    def __init__(self, parent=None):
        super(VerifyDialog, self).__init__(parent)

        self.listWidget = QListWidget()

        for i in range(100):
            item = QListWidgetItem("Item %i" % i)
            item.setCheckState(Qt.Checked) 
            self.listWidget.addItem(item)

        self.listWidget.pressed.connect(self.saveClickState)       # Get when the mouse goes down
        #self.listWidget.clicked.connect(self.saveClickState)      # Get when the mouse goes up
        self.listWidget.itemEntered.connect(self.setClickState)    # when the mouse is dragged

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
                print(item.text(),item.data)

if __name__=="__main__":
    app = QApplication(sys.argv)
    dialog = VerifyDialog()
    sys.exit(app.exec_())
