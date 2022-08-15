from etl_nyc_taxi import ETLJob
from answer_questions import AnswerQuestions
from pyspark.sql import SparkSession
from PIL import Image
from os import listdir
from os.path import isfile, join


def merge_images_vertically(images):
    '''
    This function merges images vertically
    '''
    # create two lists - one for heights and one for widths
    widths, heights = zip(*(i.size for i in images))
    width_of_new_image = min(widths)  # take minimum width
    height_of_new_image = sum(heights)
    # create new image
    new_im = Image.new('RGB', (width_of_new_image, height_of_new_image))
    new_pos = 0
    for im in images:
        new_im.paste(im, (0, new_pos))
        new_pos += im.size[1]  # position for the next image
    new_im.save('final_result.jpg')  # change the filename if you want


def run_etl(spark):
    etl = ETLJob(spark)
    etl.run()


def run_plots(spark):
    aq = AnswerQuestions(spark)
    aq.run()


def generate_pdf():
    path = 'output/'
    onlyfiles = sorted([f for f in listdir(path) if isfile(join(path, f))])
    imgs = [Image.open(path+im) for im in onlyfiles]
    merge_images_vertically(imgs)


def main():
    # spark = SparkSession.builder.master("local[*]").appName("main").getOrCreate()
    # run_etl(spark)
    # run_plots(spark)
    generate_pdf()


if __name__ == '__main__':
    main()
