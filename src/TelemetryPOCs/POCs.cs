using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using MessagePack;
using NUnit.Framework;
using ZeroFormatter;
using ZeroFormatter.Internal;

namespace TelemetryPOCs
{
    public class POCs
    {
        [SetUp]
        public void Setup()
        {
        }

        [MessagePackObject]
        public struct PackageHeader
        {
            [Key(0)] 
            public Guid ResponseToken { get; set; }
            
            [Key(1)] 
            public int PayloadSize { get; set; }
            
            [Key(2)] 
            public string PayloadType { get; set; }
        }

        [MessagePackObject]
        public class MyPackage
        {
            [Key(0)] 
            public virtual string Prop { get; set; }

            [Key(1)] 
            public virtual string Prop2 { get; set; }
        }

        [Test]
        public void PackageLayoutTest()
        {
            MyPackage package = new MyPackage();

            package.Prop = "Property 1";
            package.Prop2 = "Property 2";

            MemoryStream memoryStream = new MemoryStream();
            
            WritePackage(memoryStream, package);

            MyPackage fromStream = ReadPackage(memoryStream);
            
            Assert.IsTrue(package.Prop.Equals(fromStream.Prop));
            Assert.IsTrue(package.Prop2.Equals(fromStream.Prop2));
        }

        public void WritePackage(Stream stream, MyPackage package)
        {
            byte[] serializedPackage = MessagePackSerializer.Serialize(package);

            PackageHeader header = new PackageHeader();

            header.PayloadType = typeof(MyPackage).FullName;
            header.PayloadSize = serializedPackage.Length;
            header.ResponseToken = Guid.NewGuid();

            byte[] serializedHeader = MessagePackSerializer.Serialize(header);

            MessagePackSerializer.Typeless.
            
            byte[] serializedHeaderSize = new byte[sizeof(int)];

            BinaryUtil.WriteInt32(ref serializedHeaderSize, 0, serializedHeader.Length);

            stream.Write(serializedHeaderSize);
            stream.Write(serializedHeader);
            stream.Write(serializedPackage);
            stream.Flush();
        }
        
        private MyPackage ReadPackage(Stream stream)
        {
            stream.Position = 0;
            
            byte[] headerSizeBuffer = new byte[sizeof(int)];
            
            stream.Read(headerSizeBuffer, 0, sizeof(int));
            
            int headerSize = BinaryUtil.ReadInt32(ref headerSizeBuffer, 0);

            byte[] serializedHeader = new byte[headerSize];

            stream.Read(serializedHeader, 0, headerSize);

            PackageHeader header = MessagePackSerializer.Deserialize<PackageHeader>(serializedHeader);

            byte[] serializedPackage = new byte[header.PayloadSize];

            stream.Read(serializedPackage, 0, header.PayloadSize);
            
            return MessagePackSerializer.Deserialize<MyPackage>(serializedPackage);
        }

        private void Read(Stream stream, byte[] buffer, int numBytes)
        {
            int bytesRead = 0;
            do
            {
                int n = stream.Read(buffer, bytesRead, numBytes - bytesRead);
                if (n == 0)
                {
                    throw new Exception("Package malformed or stream ended.");
                }

                bytesRead += n;
            } while (bytesRead < numBytes);
        }
    }
}