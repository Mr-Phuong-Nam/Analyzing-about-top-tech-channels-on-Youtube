import os
import json
import numpy as np
import pandas as pd
import requests

# # Lấy API_KEY từ file API_KEY.txt
# API_KEY = open('../data/external/API_KEY.txt', 'r').read()
API_KEYS = json.load(open('../data/external/API_KEY.json', 'r'))
API_KEY = API_KEYS['Nam']

MAX_RESULTS=50
MAX_RESULTS_COMMENT=50
MAX_RESULTS_REPLY=5


main_url = 'https://www.googleapis.com/youtube/v3/'
def create_page_token(next_page_token):
    return f"pageToken={next_page_token}" if next_page_token else ""

def build_request_link_for_get_playlists(channel_id, next_page_token=None):
    """Hàm tạo request link để lấy các playlist trong một channel
    Args:
        channel_id (str):- Id của channel
                         - next_page_token
    Returns:
        link 
    """
    part=f"part=snippet,contentDetails"
    id=f"channelId={channel_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"playlists?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"playlists?",part,id,key,max_result])

def build_request_link_for_get_video_detail(video_id, next_page_token=None):
    """Hàm tạo request link để lấy các thông tin trong video
    Args:
        video_id (str):- Id của video
                        - next_page_token
    Returns:
        link 
    """
    part=f"part=contentDetails,snippet,statistics,status"
    id=f"id={video_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"videos?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"videos?",part,id,key,max_result])

def build_request_link_for_get_all_video_in_playlists(playlist_id, next_page_token=None):   
    """Hàm tạo request link để lấy các thông tin cơ bản của video trong playlist
    Args:
        video_id (str):- Id của playlist
                        - next_page_token
    Returns:
        link 
    """
    part=f"part=snippet,contentDetails"
    id=f"playlistId={playlist_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"playlistItems?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"playlistItems?",part,id,key,max_result])
def build_request_link_for_get_video_not_in_playlists(channel_id, next_page_token=None):
    """Hàm tạo request link để lấy các thông tin cơ bản của video mà không được thêm vào playlist
    Args:
        channel_id (str):- Id của channel
                        - next_page_token
    Returns:
        link 
    """
    part=f"part=snippet"
    id=f"channelId={channel_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"search?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"search?",part,id,key,max_result])
def build_request_link_for_get_replies_comment(comment_id, next_page_token=None):
    """Hàm tạo request link để lấy các thông tin của comment reply
    Args:
        comment_id (str):- Id của comment gốc
                        - next_page_token
    Returns:
        link 
    """
    part=f"part=snippet"
    id=f"parentId={comment_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"comments?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"comments?",part,id,key,max_result])

def build_request_link_for_get_comments(video_id, next_page_token=None):
    """Hàm tạo request link để lấy các thông tin của top comment ()
    Args:
        video_id (str):- Id của video
                        - next_page_token
    Returns:
        link 
    """
    part=f"part=snippet"
    id=f"videoId={video_id}"
    key=f"key={API_KEY}"
    max_result=f"maxResults={MAX_RESULTS}"
    page_token=create_page_token(next_page_token)
    if page_token:
        return "&".join([main_url+"commentThreads?",part,id,key,max_result,page_token])
    else:
        return "&".join([main_url+"commentThreads?",part,id,key,max_result])


MAX_RESULTS_COMMENT=100

def get_all_playlists(channel_id):
    """Hàm lấy tất cả các playlist của một channel và các thông tin liên quan 

    Args:
        channel_id (str): Id của channel
    Returns:
        List:Dataframe các playlist của channel với trường như sau:
            - playlist_id: Id của playlist
            - playlist_title: Tên của playlist
            - channel_id: Id của channel
            - channelTitle: Tên của channel
            - playlist_published: Ngày playlist được tạo
            - playlist_numvideo: Số video trong playlist
    """
    playlists_df = pd.DataFrame()
    next_page_token = None   # Khởi tạo next_page_token để lấy các trang tiếp theo 
    errorCode=0      #0 is success, other value is error
    message=""       #message of error

    while True:
        # Tạo URL để gửi request lấy thông tin playlist
        playlists_url=build_request_link_for_get_playlists(channel_id,next_page_token)

        # Gửi request và nhận response
        playlists_response = requests.get(playlists_url)

        # Kiểm tra xem response có thành công không
        if playlists_response.status_code!=200:
            errorCode=1
            message="Error when get playlists from channel id: "+channel_id
            break

        # Đã lấy được response
        else:
            playlists_data = playlists_response.json()
            # Kiểm tra xem response có dữ liệu không
            if 'items' in playlists_data:
                if len(playlists_data['items'])==0:
                    errorCode=2
                    message="No playlists found from channel id: "+channel_id
                    break
                # Lấy next_page_token để lặp lại yêu cầu
                next_page_token = playlists_data.get('nextPageToken')
                
                #Lấy các playlistid ,ngày published, title ,số video trong playlist
                for playlist_item in playlists_data['items']:
                    playlist_id = playlist_item['id']

                    #Tạo playlist_snippet cho dễ xử lý
                    playlist_snippet = playlist_item['snippet']
                    playlists_title=playlist_item['snippet']['title']
                    author=playlist_snippet['channelTitle']
                    playlists_published=playlist_snippet['publishedAt']
                    playlists_numvideo=playlist_item['contentDetails']['itemCount']
                    #Thêm vào dataframe
                    playlists_df=playlists_df.append({'playlist_id':playlist_id,'playlist_title':playlists_title,'channel_id':channel_id,'channelTitle':author,
                                     'playlist_published':playlists_published,'playlist_numvideo':playlists_numvideo
                                     },ignore_index=True)
                # Kiểm tra xem có trang kế tiếp không
                if not next_page_token:
                    break

            else:
                errorCode=3
                message="No items found from channel id: "+channel_id
                break

    return playlists_df,errorCode,message


def GetDetailVideo(video_id):
    """Hàm lấy các thông tin của video

    Args:
        video_id (str): Id của video
    Returns:
    Các thông tin của video với đặc tả từng phần tử sau:
        - title: Tên của video
        - published: Ngày video được đăng
        - view_count: Số lượt xem
        - like_count: Số lượt thích
        - comment_count: Số lượt bình luận
        - duration: Thời lượng video
        - definition: Độ phân giải
        - tags: Các tag của video (nếu có, nếu không có trả về None)
        - default_audio_language: Ngôn ngữ mặc định
        - madeforkid: Video dành cho trẻ em hay không
    """
    errorCode = 0  # 0 là thành công, các giá trị khác là lỗi
    message = ""  # message of error
    title = ""; published = ""; view_count = ""; like_count = ""; comment_count = ""; duration = ""; \
        definition = ""; tags = None; default_audio_language = None; madeforkid = None

    # Tạo URL endpoint để lấy thông tin video
    video_info_url=build_request_link_for_get_video_detail(video_id)
    response_video_response = requests.get(video_info_url)
    if response_video_response.status_code != 200:
        errorCode = 1
        message = "Error when get video info from video id: " + video_id
    else:
        response_video_stats = response_video_response.json()
        if 'items' in response_video_stats:
            if len(response_video_stats['items']) == 0:
                errorCode = 2
                message = "No video info found from video id: " + video_id
            else:
                items = response_video_stats['items'][0]
                title = items['snippet'].get('title')
                published = items['snippet'].get('publishedAt')
                view_count = items['statistics'].get('viewCount')
                like_count = items['statistics'].get('likeCount')
                comment_count = items['statistics'].get('commentCount')
                duration = items['contentDetails'].get('duration')
                definition = items['contentDetails'].get('definition')
                tags = items['snippet'].get('tags')
                default_audio_language = items['snippet'].get('defaultAudioLanguage')
                madeforkid = items['status'].get('madeForKids')
        else:
            errorCode = 3
            message = "No items found from video id: " + video_id

    return title, published, view_count, like_count, comment_count, \
           duration, definition, tags, default_audio_language, madeforkid, errorCode, message

def get_all_video_ids(playlist_id):
    """Hàm lấy các thông tin của các video trong playlist

    Args:
        playlist_id (str): Id của playlist
    Returns:
    Dataframe các video trong playlist với các trường như sau:
        - playlist_id: Id của playlist
        - video_id: Id của video
        #Các thông tin bên dưới lấy từ hàm GetDetailVideo
        - title: Tên của video
        - published: Ngày video được đăng
        - view_count: Số lượt xem
        - like_count: Số lượt thích
        - comment_count: Số lượt bình luận
        - duration: Thời lượng video
        - definition: Độ phân giải
        - tags: Các tag của video
        - default_audio_language: Ngôn ngữ mặc định
        - madeforkid: Video dành cho trẻ em hay không
    """
    videos_df = pd.DataFrame()
    next_page_token = None
    errorCode=0      #0 là thành công, các giá trị khác là lỗi
    message=""       #message of error
    while True:
        # Tạo URL endpoint để lấy danh sách video trong playlist
        playlist_items_url=build_request_link_for_get_all_video_in_playlists(playlist_id,next_page_token)

        # Gửi request và nhận response
        playlist_items_response = requests.get(playlist_items_url)
        if playlist_items_response.status_code!=200:
            errorCode=1
            message="Error when get videos from playlist id: "+playlist_id
            break
        
        # Đã lấy được response
        playlist_items_data = playlist_items_response.json()

        # Kiểm tra xem response có dữ liệu không
        if 'items' in playlist_items_data:

            # Lấy các video id từ response
            for item in playlist_items_data['items']:
                video_id = item['contentDetails']['videoId']

                errorCodeVideoDetail=0
                messageVideoDetail=""
                title,published,view_count,like_count,comment_count,\
                duration,definition,tags,default_audio_language,madeforkid,\
                errorCodeVideoDetail,messageVideoDetail=GetDetailVideo(video_id)

                if errorCodeVideoDetail!=0:
                    # errorCode=2
                    # message="Error when get video detail from video id: "+video_id
                    # break
                    continue     #Bỏ qua video này nếu lỗi 
                
                #Thêm vào dataframe
                videos_df = videos_df.append({'playlist_id':playlist_id,'video_id': video_id,'title':title,'published':published,
                                              'view_count':view_count,'like_count':like_count,
                                              'comment_count':comment_count,'duration':duration,
                                              'definition':definition,'tags':tags,'default_audio_language':default_audio_language,
                                              'madeforkid':madeforkid}, ignore_index=True)
            # Lấy next_page_token để lặp lại yêu cầu
            next_page_token = playlist_items_data.get('nextPageToken')

            # Kiểm tra xem có trang kế tiếp không
            if not next_page_token:
                break
        else:
            errorCode=3
            message="No items found from playlist id: "+playlist_id
            break
            
    return videos_df,errorCode,message

def get_all_video_not_in_playlists(channel_id,videos_in_playlists:pd.DataFrame):
    """Hàm lấy các thông tin của các video không có trong playlist

    Args:
        channel_id (str): Id của channel
    Returns:
    Dataframe các video không có trong playlist với các trường như sau:
        - playlist_id: Id của playlist
        - video_id: Id của video
        #Các thông tin bên dưới lấy từ hàm GetDetailVideo
        - title: Tên của video
        - published: Ngày video được đăng
        - view_count: Số lượt xem
        - like_count: Số lượt thích
        - comment_count: Số lượt bình luận
        - duration: Thời lượng video
        - definition: Độ phân giải
        - tags: Các tag của video
        - default_audio_language: Ngôn ngữ mặc định
        - madeforkid: Video dành cho trẻ em hay không
    """
    videos_df = pd.DataFrame()
    next_page_token = None
    errorCode=0      #0 là thành công, các giá trị khác là lỗi
    message=""       #message of error
    while True:
        # Tạo URL endpoint để lấy danh sách video trong playlist
        videos_url=build_request_link_for_get_video_not_in_playlists(channel_id,next_page_token)

        # Gửi request và nhận response
        response = requests.get(videos_url)
        if response.status_code!=200:
            errorCode=1
            message="Error when get videos from channel id: "+channel_id
            break
        
        # Đã lấy được response
        data = response.json()

        # Kiểm tra xem response có dữ liệu không
        if 'items' in data:

            # Lấy các video id từ response
            for item in data['items']:
                if item['id'].get('kind')=='youtube#video':
                    video_id = item['id'].get('videoId')
                    if video_id not in videos_in_playlists['video_id'].values:
                        errorCodeVideoDetail=0
                        messageVideoDetail=""
                        title,published,view_count,like_count,comment_count,\
                        duration,definition,tags,default_audio_language,madeforkid,\
                        errorCodeVideoDetail,messageVideoDetail=GetDetailVideo(video_id)

                        if errorCodeVideoDetail!=0:
                            # errorCode=2
                            # message="Error when get video detail from video id: "+video_id
                            # break
                            continue
                        videos_df=videos_df.append({'playlist_id':None,'video_id': video_id,'title':title,'published':published,'view_count':view_count,'like_count':like_count,
                                                'comment_count':comment_count,'duration':duration,'definition':definition,'tags':tags,
                                                'default_audio_language':default_audio_language,'madeforkid':madeforkid}, ignore_index=True)
            # Lấy next_page_token để lặp lại yêu cầu  
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
        else:
            errorCode=3
            message="No items found from channel id: "+channel_id
            break
    return videos_df,errorCode,message






def GetCommentReply(comment_id,video_id):
    """Hàm lấy các thông tin của các reply trong comment

    Args:
        comment_id (str): Id của comment
        video_id (str): Id của video
    Returns:
    Dataframe các reply trong comment với các trường như sau:
        - Comment_id: Id của comment
        - Reply_for: Id của comment cha 
        - Type : mặc định là reply (=2)
        - video_id: Id của video   
        - total_reply: Tổng số reply của comment  (mặc định là 0 , vì chỉ lấy reply của comment top)
        - like_count: Số lượt thích 
        - published_at: Ngày comment được đăng
        - textdisplay: Nội dung của comment
        - publishedat: Id của người đăng comment
        - updatedat: Ngày comment được cập nhật
    """
    next_page_token = None
    errorCode=0      #0 là thành công, các giá trị khác là lỗi
    message=""       #message of error
    reply_cmt_df=pd.DataFrame()
    while True:
        # Tạo URL endpoint để lấy danh sách reply của comment
        replies_url=build_request_link_for_get_replies_comment(comment_id,next_page_token)
        # Gửi request và nhận response
        replies_response = requests.get(replies_url)

        # Kiểm tra xem response có thành công không
        if replies_response.status_code!=200:
            errorCode=1
            message="Error when get replies from comment id: "+comment_id
            break
        
        # Đã lấy được response
        replies_data = replies_response.json()

        # Kiểm tra xem response có dữ liệu không
        if 'items' in replies_data:
            if len(replies_data['items'])==0:
                errorCode=2
                message="No replies found from comment id: "+comment_id
                break
            for item in replies_data['items']:
                reply_cmt_id=item['id']
                reply_for=comment_id
                type_cmt=2   #reply nen type=2
                total_reply=0  #mặc định là 0 , vì chỉ lấy reply của comment top
                author=item['snippet'].get('authorDisplayName')
                like_count=item['snippet'].get('likeCount')
                published_at=item['snippet'].get('publishedAt')
                textdisplay=item['snippet'].get('textDisplay')
                updatedat=item['snippet'].get('updatedAt')

                if reply_cmt_df.shape[0] >= MAX_RESULTS_REPLY:
                    return reply_cmt_df,errorCode,message
                reply_cmt_df=reply_cmt_df.append({'Comment_id':reply_cmt_id,'author':author,'Reply_for':reply_for,'Type':type_cmt,'video_id':video_id,
                                                  'total_reply':total_reply,'like_count':like_count,'published_at':published_at,
                                                  'textdisplay':textdisplay,'updatedat':updatedat},ignore_index=True)
            # Lấy next_page_token để lặp lại yêu cầu
            next_page_token = replies_data.get('nextPageToken')
        else:
            errorCode=3
            message="No items found from comment id: "+comment_id
            break
        # Kiểm tra xem có trang kế tiếp không
        if not next_page_token:
            break
    return reply_cmt_df,errorCode,message

def GetComment(video_id):
    """
    Hàm lấy các thông tin của comment trong video 
    Args:
        video_id (str): Id của video
    Returns:
    Dataframe các comment trong video với các trường như sau:
        - Comment_id: Id của comment
        - Reply_for: Id của comment cha 
        - Type : Loại của comment (top-level comment (1) hay reply(2)) 
        - video_id: Id của video
        - total_reply: Tổng số reply của comment
        - like_count: Số lượt thích
        - published_at: Ngày comment được đăng
        - textdisplay: Nội dung của comment
        - publishedat: Id của người đăng comment
        - updatedat: Ngày comment được cập nhật
    """
    next_page_token = None
    comments_df = pd.DataFrame()
    errorCode=0  #0 là thành công ,khác 0 là thất bại 
    message=""
    count_comment=0
    while True:
        # Tạo URL endpoint để lấy danh sách comment của video
        comments_url=build_request_link_for_get_comments(video_id,next_page_token)

        # Gửi request và nhận response
        comments_response = requests.get(comments_url)

        if comments_response.status_code!=200:
            errorCode=1
            message="Error get response from video id: "+video_id+" with status code: "+str(comments_response.status_code)
            break
        #Đã nhận được response
        comments_data = comments_response.json()
        # Kiểm tra xem response có dữ liệu không
        if 'items' in comments_data:
            if len(comments_data['items'])==0:
                errorCode=2
                message="Don't have comment in video id: "+video_id
            # Lấy danh sách comment từ response
            for item in comments_data['items']:
                #Xử lý top-level comment
                top_cmt_id=item['id']
                reply_for=None
                type=1  #1 là top-level comment
                author=item['snippet']['topLevelComment']['snippet'].get('authorDisplayName')
                total_reply=item['snippet'].get('totalReplyCount')
                top_comment_data=item['snippet']['topLevelComment']['snippet']
                like_count=top_comment_data.get('likeCount')
                published_at=top_comment_data.get('publishedAt')
                textdisplay=top_comment_data.get('textDisplay')
                updatedat=top_comment_data.get('updatedAt')

                #Kiem tra so luong comment hien tai da vuot qua gioi han chua
                count_comment=comments_df.shape[0]
                if count_comment>=MAX_RESULTS_COMMENT:
                    return comments_df,errorCode,message
                comments_df = comments_df.append({'Comment_id':top_cmt_id,'author':author,'Reply_for':reply_for,'Type':type,'video_id':video_id,
                                                  'total_reply':total_reply,'like_count':like_count,'published_at':published_at,
                                                  'textdisplay':textdisplay,'updatedat':updatedat}, ignore_index=True)
                #Xử lý reply
                if total_reply==0:
                    continue
               
                replies_df,errorCodeReply,messageReply=GetCommentReply(top_cmt_id,video_id)
                if errorCodeReply!=0:
                    # errorCode=2
                    # message="Error when get replies from comment id: "+top_cmt_id
                    # break
                    continue
                #Kiem tra so luong comment hien tai da vuot qua gioi han chua
                count_comment=comments_df.shape[0]
                if count_comment>=MAX_RESULTS_COMMENT:
                    return comments_df,errorCode,message
                comments_df=pd.concat([comments_df,replies_df],ignore_index=True,axis=0)
            # Lấy next_page_token để lặp lại yêu cầu
            next_page_token = comments_data.get('nextPageToken')

            # Kiểm tra xem có trang kế tiếp không
            if not next_page_token:
                break
        else:
            errorCode=3
            message="No items found from video id: "+video_id
            break
    return comments_df,errorCode,message

def make_dataset_playlist_video(channel, channel_id):
    """Hàm lấy tất cả các video của channel

    Args:
        channel_id (str): Id của channel
    Returns:
    Dataframe các video của channel với các trường như sau:
        - playlist_id: Id của playlist
        - playlist_title: Tên của playlist
        - playlist_published: Ngày playlist được tạo
        - playlist_numvideo: Số video trong playlist
        - video_id: Id của video
        #Các thông tin bên dưới lấy từ hàm GetDetailVideo
        - title: Tên của video
        - published: Ngày video được đăng
        - view_count: Số lượt xem
        - like_count: Số lượt thích
        - comment_count: Số lượt bình luận
        - duration: Thời lượng video
        - definition: Độ phân giải
        - tags: Các tag của video
        - default_audio_language: Ngôn ngữ mặc định
        - madeforkid: Video dành cho trẻ em hay không
    """
    # Tạo dataframe để lưu thông tin các playlist của channel
    playlists_df = pd.DataFrame(columns=['playlist_id', 'playlist_title', 'channel_id', 'channelTitle',\
                                         'playlist_published','playlist_numvideo'])
    playlists_df,errorCode,message=get_all_playlists(channel_id)
    result=0
    if errorCode!=0:
        print(message)
        result=errorCode
    # Lấy tất cả video trên playlists
    no_playlist_rs=2
    videos_df = pd.DataFrame(columns=['playlist_id', 'video_id', 'title', 'published', 'view_count',
       'like_count', 'comment_count', 'duration', 'definition', 'tags',
       'default_audio_language', 'madeforkid'])
    if result!=no_playlist_rs:
        for playlist_id in playlists_df['playlist_id']:
            videos_df_temp,errorCodeVideo,messageVideo=get_all_video_ids(playlist_id)
            if errorCodeVideo!=0:
                # errorCode=2
                print(messageVideo)
                # break
                continue
            videos_df=pd.concat([videos_df,videos_df_temp],ignore_index=True)
    # Lấy tất cả video không có trong playlists
    videos_df_temp,errorCodeVideo,messageVideo=get_all_video_not_in_playlists(channel_id,videos_df)
    if errorCodeVideo!=0:
        # errorCode=2
        print(messageVideo)
        # break
        result=errorCodeVideo
    videos_df=pd.concat([videos_df,videos_df_temp],ignore_index=True)

    if result!=no_playlist_rs:
        # Nôi playlist_df và videos_df lại thành một dataframe video duy nhất
        videos_df = pd.merge(videos_df, playlists_df, on='playlist_id', how='left')
    
    # Save dataframe thành file csv
    videos_df.to_csv(f'../data/raw/{channel}_videos.csv', index=False)
    return result
def make_dataset_comment(channel):
    """Hàm lấy tất cả các comment của channel
    Args:
        channel_id (str): Id của channel
    Returns:
    Dataframe các comment của channel với các trường như sau:
        - Comment_id: Id của comment
        - author: Tên của người đăng comment
        - Reply_for: Id của comment cha 
        - Type : Loại của comment (top-level comment (1) hay reply(2)) 
        - video_id: Id của video
        - total_reply: Tổng số reply của comment
        - like_count: Số lượt thích
        - published_at: Ngày comment được đăng
        - textdisplay: Nội dung của comment
        - publishedat: Id của người đăng comment
        - updatedat: Ngày comment được cập nhật
    """
    # Tạo dataframe để lưu thông tin các playlist của channel
    comments_df = pd.DataFrame()
    videos_df_temp=pd.read_csv(f'../data/raw/{channel}_videos.csv')
    for video_id in videos_df_temp['video_id']:
        comments_df_temp,errorCodeComment,messageComment=GetComment(video_id)
        if errorCodeComment!=0:
            # errorCode=2
            print(messageComment)
            # break
            continue
        comments_df=pd.concat([comments_df,comments_df_temp],ignore_index=True)
    # Save dataframe thành file csv
    comments_df.to_csv(f'../data/raw/{channel}_comments.csv', index=False)
    return 0

