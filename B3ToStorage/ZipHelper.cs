using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace B3ToStorage
{
    public static class ZipHelper
    {
        public static string Unzip(byte[] zipByteArray)
        {
            using (var dataZipStream = new MemoryStream(zipByteArray))
            {
                using (var dataZipFile = new ZipArchive(dataZipStream))
                {
                    var zipEntry = dataZipFile.Entries.FirstOrDefault();
                    if (zipEntry != null)
                    {
                        using (var entryStream = zipEntry.Open())
                        {
                            using (var ms = new MemoryStream())
                            {
                                entryStream.CopyTo(ms);
                                var unzippedArray = ms.ToArray();

                                return Encoding.Default.GetString(unzippedArray);
                            }
                        }
                    }
                }
                return null;
            }
        }
    }
}
