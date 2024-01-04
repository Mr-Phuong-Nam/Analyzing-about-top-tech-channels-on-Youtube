# DS_FINAL_PROJ
Final project of Introdution to Data Science

## Table of Contents

 - [Information](#information)
 - [Introduction](#introduction)
 - [How can we collect data?](#how-can-we-collect-data)
 - [Structure](#structure)
 - [Environment](#environment)
 - [Usage](#usage)
 - [Deploy](#deploy)

## Information

| Student ID | Name                   | Email                         | 
|------------|------------------------|-------------------------------|
| 21120504   | Nguyễn Phương Nam      | 21120504@student.hcmus.edu.vn |
| 21120516   | Võ Bá Hoàng Nhất       | 21120516@student.hcmus.edu.vn |
| 21120529   | Nguyễn Gia Phúc        | 21120529@student.hcmus.edu.vn |

## Introduction

- This is the Final Project of class **Introduction to Data Science - 21_21** (2023).

- YouTube stands out as a widely embraced platform for video consumption, seamlessly integrating into our daily routines. Whether for entertainment, education, or staying informed with the latest news, YouTube has become an integral part of our lives. As students specializing in Data Science, we aspire to seamlessly incorporate Data Science topics into our daily existence by leveraging YouTube videos. Thus, our decision was made to delve into an analysis of Data Science channels on YouTube. Exploring their operational dynamics, thematic focuses, and conducting comprehensive comparisons will empower us to identify the most fitting channels. This strategic approach aims to seamlessly integrate Data Science content into our daily lives.marketplace and contribute valuable insights to the field of used car sales.

## How can we collect data?

- Initially, we conducted research to identify popular Data Science channels on the internet, selecting some based on personal preferences. Subsequently, we obtained the channel IDs using [this website](https://www.streamweasels.com/tools/youtube-channel-id-and-user-id-convertor/). 

- Next, utilizing these channel IDs, we made requests to the Youtube Data API v3 to retrieve all video IDs associated with these channels.

- Following that, we utilized the video IDs to send additional requests to the Youtube Data API v3, acquiring detailed information about each video. Subsequently, we stored this information in a CSV file.

- Lastly, we employed the video IDs once more to send requests to the Youtube Data API v3, this time retrieving all the comments for these videos. The comments were then stored in a separate CSV file.

- Link to the api: [Youtube Data API v3](https://developers.google.com/youtube/v3)


## Structure

 

```

├── data
│   ├── external
│   │   ├── API.json
│   │   ├── Channel_IndentifialName.json
│   │   ├── channel_id.json
│   │   └── keywords.txt
│   ├── processed
│   │   ├── comments_sentiment.csv
│   │   ├── df_channels_processed.csv
│   │   ├── df_videos_processed.csv
│   │   └── df_comments_processed.csv
│   └── raw <- All raw dataframes used to combine into processed dataframes
├── deploy
│   ├── app.py
│   │── materials
│   │   ├── mlb.joblib
│   │   ├── tags.json
│   └── templates
│      ├── index.html
│      └── result.html
├── models
│   └── model.joblib
├── notebooks
│   ├── 1.0-data-collecting.ipynb
│   ├── 2.0-preprocessing.ipynb
│   ├── 3.0-visualization-eda.ipynb
│   └── 4.0-data-modelling.ipynb
├── reports
│   └── Slide_PRESENTATION.pdf
├── src
│   └── data_module
│       ├── make_dataset.py
│       └── read_processed_df.py
├── min_ds-env.yml
├── PlainTask.pdf
└── README.md

```

## Environment
Run the following command to create a new environment from the `min_ds-env.yml` file:

```
conda env create -f min_ds-env.yml
```

Run the following command to activate the environment:

```
conda activate min_ds-env
```

## Usage

1. Clone repository to your device

```
https://github.com/Mr-Phuong-Nam/Analyzing-about-top-tech-channels-on-Youtube.git
```
2. Create a new environment from the `min_ds-env.yml` file and activate the environment

3. Run each notebook in the `notebooks` folder in order

# Deploy
1. Install the necessary libraries for the operating environment: sklearn, joblib, flask,....
2. Navigate to the `deploy` directory.
In the terminal, use the cd command to go to the `deploy` directory where your project files are located.
3.  Run the Flask application
In the terminal, execute the following command to start the Flask development server:
```sh
	flask run
```
4. Navigate to the link shown in the terminal
-   The `flask run` command will output a URL in the terminal, typically in the format `http://127.0.0.1:5000/`.
-   Copy and paste this URL into your web browser to access the deployed application.