#!/usr/bin/python
import cv2 as cv
import os
import sys
import numpy as np

print('Number of arguments:', len(sys.argv), 'arguments.')
print('Argument List:', str(sys.argv))
input = os.path.abspath(str(sys.argv[1]))
output = os.path.abspath(str(sys.argv[2]))
print('input',input)

# img = cv.imread("/Users/pemartin/Scripts/datalad-test/Datalad-101/inputs/I1/1280px-Philips_PM5544.svg.png")
img = cv.imread(input)

if img is None:
    sys.exit("Could not read the image.")


cv.imshow("Display window", img)
k = cv.waitKey(0)


kernel = np.ones((5,5),np.float32)/25  #apply a simple blurring filter with a 5x5 kernel
dst = cv.filter2D(img,-1,kernel)

cv.imshow("blurred", dst)
k = cv.waitKey(0)


# cv.imwrite("/Users/pemartin/Scripts/datalad-test/Datalad-101/outputs/O1/step1.png", img)
cv.imwrite(output, img)

cv.destroyAllWindows()



