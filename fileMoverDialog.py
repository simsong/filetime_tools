import sys
from PyQt5.QtCore import QDate, QSize, Qt, QTimer
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

# For ideas, see:
# http://nullege.com/codes/show/src@p@y@pyqt5-HEAD@examples@dialogs@configdialog@configdialog.py/220/PyQt5.QtWidgets.QListWidgetItem.setTextAlignment/python

# https://doc.qt.io/qt-5/qlistwidget.html#itemEntered

class FileMoverListItem(QListWidgetItem):
    def __init__(self, source, dest ):
        super(QListWidgetItem, self).__init__("{} => {}".format(source,dest))
        self.source = source
        self.dest   = dest

class VerifyDialog(QDialog):
    def __init__(self, movelist=[], callback=None, parent=None):
        super(VerifyDialog, self).__init__(parent)

        self.listWidget = QListWidget()
        self.callback  = callback

        for (source,dest) in movelist:
            item = FileMoverListItem(source,dest)
            item.setCheckState(Qt.Checked) 
            self.listWidget.addItem(item)

        self.listWidget.pressed.connect(self.saveClickState)       # Get when the mouse goes down
        self.listWidget.itemEntered.connect(self.setClickState)    # when the mouse is dragged
        self.newState = Qt.Checked
        self.inPressed = False

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

    def saveClickState(self, index):
        self.inPressed = True
        item = self.listWidget.currentItem()
        self.newState = Qt.Checked if item.checkState()==Qt.Unchecked else Qt.Unchecked
        item.setCheckState(self.newState)

    def setClickState(self, item):
        if self.inPressed:
            item.setCheckState(self.newState)
            
    def exec(self):
        self.allItems = self.listWidget.findItems("",Qt.MatchContains)
        self.timer = QTimer()
        self.timer.setInterval(0)
        self.timer.setSingleShot(False)
        self.timer.timeout.connect(self.execNext)
        self.timer.start(0)

    def execNext(self):
        item = self.allItems.pop(0)
        self.listWidget.scrollToItem(item,QAbstractItemView.PositionAtBottom)
        if item.checkState():
            if self.callback:
                self.callback(item)
            item.setBackground(QColor("cyan"))
            item.setCheckState(Qt.Unchecked)
        if not self.allItems:
            self.timer.stop()
        

if __name__=="__main__":
    def mlcb(item):
        import time
        print("{} => {}".format(item.source,item.dest))
        time.sleep(.05)
    # Debug
    movelist = [[f"this {i}", f"that {i}"] for i in range(1,100)]
    app = QApplication(sys.argv)
    dialog = VerifyDialog(movelist,mlcb)
    sys.exit(app.exec_())
