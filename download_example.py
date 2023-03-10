import glob
import os
import wave
from datetime import datetime
from orca_hls_utils import DateRangeHLSStream, datetime_utils
from orca_hls_utils.datetime_utils import get_unix_time_from_datetime_pdt

if __name__ == '__main__':

    stream_base = 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab'
    polling_interval = 2 * 3600 # [s] time period to extract from start_unix_time

    start_unix_time = get_unix_time_from_datetime_pdt(datetime.strptime("2022-09-03 16:15:00", "%Y-%m-%d %H:%M:%S"))  # -> to do

    end_unix_time = start_unix_time + polling_interval
    wav_dir = 'prout'
    if not os.path.exists(wav_dir):
        os.makedirs(wav_dir)

    stream = DateRangeHLSStream.DateRangeHLSStream(stream_base=stream_base,
                                polling_interval=polling_interval,
                                start_unix_time=start_unix_time,
                                end_unix_time=end_unix_time,
                                wav_dir=wav_dir,
                                overwrite_output=True,
                                real_time=False)
    nb_folders_ori = len(stream.valid_folders)
    for _ in range(nb_folders_ori):
        stream.get_next_clip()

    if len(stream.valid_folders) > 1:
        infiles = glob.glob(os.path.join(wav_dir, '*.wav'))
        outfile = infiles[-1].replace('.wav', '_concat.wav')

        data = []
        for infile in infiles:
            w = wave.open(infile, 'rb')
            data.append([w.getparams(), w.readframes(w.getnframes())])
            w.close()

        output = wave.open(outfile, 'wb')
        output.setparams(data[0][0])
        for i in range(len(data)):
            output.writeframes(data[i][1])
        output.close()

        print('ok')



# # start_unix_time = 1657540800 # 11/07/2022 8am
# # end_unix_time = 1657569600 # 11/07/2022 4pm
# start_unix_time = 1657551600 # 11/07/2022 11am
# end_unix_time = 1657558800 # 11/07/2022 1pm bucket 1657546219
# # start_unix_time = 1657539000 # 11/07/2022 11am
# # end_unix_time = 1657553400 # 11/07/2022 1pm bucket 1657546219
# # start_unix_time = 1659121800 # 29/07/2022 3pm
# # end_unix_time = 1659132600 # 29/07/2022 6pm 2 buckets: [1659101419, 1659123019]
# # start_unix_time = 1659752820 # 05/08/2022 10:27pm
# # end_unix_time = 1659758400 # 06/08/2022 12am bucket 1659749419
# # start_unix_time = 1653426000 # 24/05/2022 5pm
# # end_unix_time = 1653433200 # 24/05/2022 6pm bucket 1659749419
# # start_unix_time = 1646166600 # 01/03/2022 3:30pm
# # end_unix_time = 1646173800 # 01/03/2022 5:30pm bucket [1646145019=9h30-15h30, 1646166619]
# # annot_unix_time = datetime.strptime("2022-07-11 13:30:00", "%Y-%m-%d %H:%M:%S").timestamp()
# # annot_unix_time = 1657567800  # "2022-07-11 15:30:00"
# start_unix_time = 1662634800 # 080922 6:00
# start_unix_time = 1668269923 # 121122 8:00:23 PDT

# does not work 2022-07-11 18:20:00 / 2022-09-07 18:30:00 /