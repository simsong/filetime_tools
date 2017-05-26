import sys
import os
from PyQt5.QtCore import pyqtSlot,Qt, QDate, QSize, QTimer
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *

# For ideas, see:
# http://nullege.com/codes/show/src@p@y@pyqt5-HEAD@examples@dialogs@configdialog@configdialog.py/220/PyQt5.QtWidgets.QListWidgetItem.setTextAlignment/python

# https://doc.qt.io/qt-5/qlistwidget.html#itemEntered

class FileMoverListItem(QListWidgetItem):
    def __init__(self, source, dest ):
        text = "{}: {}   →   {}".format(os.path.dirname(source),
                                     os.path.basename(source),
                                     os.path.basename(dest))
        super(QListWidgetItem, self).__init__(text)
        self.source = source
        self.dest   = dest

class VerifyDialog(QDialog):
    def __init__(self, movelist=[], callback=None, parent=None):
        super(VerifyDialog, self).__init__(parent)

        self.movelist = movelist
        self.callback  = callback
        self.createTable()
        self.newState = Qt.Checked
        self.inPressed = False

        runButton = QPushButton("Run")
        runButton.clicked.connect(self.exec)

        cancelButton = QPushButton("Cancel")
        cancelButton.clicked.connect(self.close)
  
        horizontalLayout = QHBoxLayout()
        horizontalLayout.addWidget(self.tableWidget, 1)
  
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


    def createTable(self):
        # Create table
        self.tableWidget = QTableWidget()
        self.tableWidget.setRowCount(len(self.movelist))
        self.tableWidget.setColumnCount(4)
        self.tableWidget.setHorizontalHeaderLabels(['','Dir','Source','Dest'])
        for i in range(len(self.movelist)):
            (source,dest) = self.movelist[i]
            item = QTableWidgetItem()
            item.setCheckState(Qt.Checked)
            self.tableWidget.setItem(i,0, item)
            self.tableWidget.setItem(i,1, QTableWidgetItem(os.path.dirname(source)))
            self.tableWidget.setItem(i,2, QTableWidgetItem(os.path.basename(source)))
            self.tableWidget.setItem(i,3, QTableWidgetItem(os.path.basename(dest)))

        # table selection change
        self.tableWidget.itemSelectionChanged.connect(self.on_changed)
        self.tableWidget.cellPressed.connect(self.on_cell_pressed)
        self.tableWidget.itemClicked.connect(self.on_item_clicked)
        #self.tableWidget.resizeRowsToContents()
        self.tableWidget.resizeColumnsToContents()
        self.newState = None

    @pyqtSlot()
    def on_changed(self):
        # Handles initial click and drag. Sets the entire row
        for r in self.tableWidget.selectedRanges():
            self.tableWidget.setRangeSelected(
                QTableWidgetSelectionRange(r.topRow(),0,
                                           r.bottomRow(),self.tableWidget.columnCount()-1),
                True)
            for row in range(r.topRow(),r.bottomRow()+1):
                item = self.tableWidget.item(row,0)
                if item and self.newState!=None:
                    item.setCheckState(self.newState)
            

    @pyqtSlot(int,int)
    def on_cell_pressed(self,row,column):
        self.flipClickState(row)

    @pyqtSlot(int)
    def flipClickState(self,row):
        item = self.tableWidget.item(row,0)
        if item:
            self.newState = Qt.Checked if item.checkState()==Qt.Unchecked else Qt.Unchecked
            item.setCheckState(self.newState)
            
    @pyqtSlot()
    # Called on release; clear the selection
    def on_item_clicked(self,item):
        for range in self.tableWidget.selectedRanges():
            self.tableWidget.setRangeSelected(range,False)

    def exec(self):
        self.currentRow = 0
        self.timer = QTimer()
        self.timer.setInterval(0)
        self.timer.setSingleShot(False)
        self.timer.timeout.connect(self.execNext)
        self.timer.start(0)

    def execNext(self):
        item = self.tableWidget.item(self.currentRow,0)
        if item:
            self.tableWidget.scrollToItem(item)
            if item.checkState():
                if self.callback:
                    self.callback(self.movelist[self.currentRow][0],self.movelist[self.currentRow][1])
                for col in range(0,self.tableWidget.columnCount()):
                    item.setBackground(QColor("cyan"))
                item.setCheckState(Qt.Unchecked)
            self.currentRow += 1
        if not item or self.currentRow >= self.tableWidget.rowCount():
            self.timer.stop()
        
if __name__=="__main__":
    def cb(item):
        import time
        print("{} => {}".format(item.source,item.dest))
    movelist = [[f"a/b/c/d/source{i}", f"a/b/c/d/dest{i}"] for i in range(1,100)]
    app = QApplication(sys.argv)
    dialog = VerifyDialog(movelist,cb)
    sys.exit(app.exec_())
