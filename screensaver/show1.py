#!/usr/bin/python

# use a Tkinter label as a panel/frame with a background image
# note that Tkinter only reads gif and ppm images
# use the Python Image Library (PIL) for other image formats
# free from [url]http://www.pythonware.com/products/pil/index.htm[/url]
# give Tkinter a namespace to avoid conflicts with PIL
# (they both have a class named Image)

import Tkinter as tk
from PIL import Image, ImageTk
from Tkinter.ttk import Frame, Button, Style
import time

class Example():
    def __init__(self):
        self.root = tk.Tk()
        self.root.title('My Pictures')

        # pick an image file you have .bmp  .jpg  .gif.  .png
        # load the file and covert it to a Tkinter image object
        imageFile = "babyAce.jpg"
        self.image1 = ImageTk.PhotoImage(Image.open(imageFile))
        self.image2 = ImageTk.PhotoImage(Image.open("baby-marti.jpg"))

        # get the image size
        w = self.image1.width()
        h = self.image1.height()

        # position coordinates of root 'upper left corner'
        x = 0
        y = 0

        # make the root window the size of the image
        self.root.geometry("%dx%d+%d+%d" % (w, h, x, y))

        # root has no image argument, so use a label as a panel
        self.panel1 = tk.Label(self.root, image=self.image1)
        self.display = self.image1
        self.panel1.pack(side=tk.TOP, fill=tk.BOTH, expand=tk.YES)
        print "Display image1"
        self.root.after(30000, self.update_image)
        self.root.mainloop()

def update_image(self):
    if self.display == self.image1:
        self.panel1.configure(image=self.image2)
        print "Display image2"
        self.display = self.image2
    else:
        self.panel1.configure(image=self.image1)
        print "Display image1"
        self.display = self.image1
    self.root.after(30000, self.update_image)       # Set to call again in 30 seconds

def main():
    app = Example()

if __name__ == '__main__':
    main()
