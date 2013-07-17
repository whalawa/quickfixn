using System.Net.Sockets;
using Common.Logging;

namespace QuickFix
{
    /// <summary>
    /// TODO merge with SocketInitiatorThread
    /// </summary>
    public class SocketReader
    {
        public const int BUF_SIZE = 4096;
        private byte[] readBuffer_ = new byte[BUF_SIZE];
        private Parser parser_ = new Parser();
        private Session qfSession_ = null;
        private TcpClient tcpClient_;
        private ClientHandlerThread responder_;
        private static readonly Common.Logging.ILog log_ = LogManager.GetCurrentClassLogger();

        public SocketReader(TcpClient tcpClient, ClientHandlerThread responder)
        {
            tcpClient_ = tcpClient;
            responder_ = responder;
        }

        /// <summary> FIXME </summary>
        public void Read()
        {
            try
            {
                if (tcpClient_.Client.Poll(1000000, SelectMode.SelectRead)) // one-second timeout
                {
                    int bytesRead = tcpClient_.Client.Receive(readBuffer_);
                    if (bytesRead < 1)
                        throw new SocketException(System.Convert.ToInt32(SocketError.ConnectionReset));
                    parser_.AddToStream(ref readBuffer_, bytesRead);
                }
                else if (null != qfSession_)
                {
                    qfSession_.Next();
                }

                ProcessStream();
            }
            catch (MessageParseError e)
            {
                HandleException(qfSession_, e, tcpClient_);
            }
            catch (System.Exception e)
            {
                HandleException(qfSession_, e, tcpClient_);
                throw e;
            }
        }

        public void OnMessageFound(string msg)
        {
            ///Message fixMessage;

            try
            {
                if (null == qfSession_)
                {
                    qfSession_ = Session.LookupSession(Message.GetReverseSessionID(msg));
                    if (null == qfSession_)
                    {
                        this.Log("ERROR: Disconnecting; received message for unknown session: " + msg);
                        DisconnectClient();
                        return;
                    }
                    else
                    {
                        if (!HandleNewSession(msg))
                            return;
                    }
                }

                try
                {
                    qfSession_.Next(msg);
                }
                catch (System.Exception e)
                {
                    this.Log("Error on Session '" + qfSession_.SessionID + "': " + e.ToString());
                }
            }
            catch (InvalidMessage e)
            {
                HandleBadMessage(msg, e);
            }
            catch (MessageParseError e)
            {
                HandleBadMessage(msg, e);
            }
        }

        protected void HandleBadMessage(string msg, System.Exception e)
        {
            try
            {
                if (Fields.MsgType.LOGON.Equals(Message.GetMsgType(msg)))
                {
                    this.Log("ERROR: Invalid LOGON message, disconnecting: " + e.Message);
                    DisconnectClient();
                }
                else
                {
                    this.Log("ERROR: Invalid message: " + e.Message);
                }
            }
            catch (InvalidMessage)
            { }
        }

        protected bool ReadMessage(out string msg)
        {
            try
            {
                return parser_.ReadFixMessage(out msg);
            }
            catch(MessageParseError e)
            {
                msg = "";
                throw e;
            }
        }

        protected void ProcessStream()
        {
            string msg;
            while (ReadMessage(out msg))
                OnMessageFound(msg);
        }

        protected static void DisconnectClient(TcpClient client)
        {
            client.Client.Close();
            client.Close();
        }

        protected void DisconnectClient()
        {
            DisconnectClient(tcpClient_);
        }

        protected bool HandleNewSession(string msg)
	    {
		    if(qfSession_.HasResponder)
		    {
                qfSession_.MessageLog.OnIncoming(msg);
                log_.ErrorFormat("Multiple logons/connections for this session ({0}) are not allowed ({1})", qfSession_.SessionID, tcpClient_.Client.RemoteEndPoint);
			    qfSession_ = null;
                DisconnectClient();
			    return false;
		    }
            log_.InfoFormat("{0} Socket Reader {1} accepting session {2} from {3}", qfSession_.SessionID, GetHashCode(), qfSession_.SessionID, tcpClient_.Client.RemoteEndPoint);
            /// FIXME do this here? qfSession_.HeartBtInt = QuickFix.Fields.Converters.IntConverter.Convert(message.GetField(Fields.Tags.HeartBtInt)); /// FIXME
            log_.InfoFormat("{0} Acceptor heartbeat set to {1} seconds", qfSession_.SessionID, qfSession_.HeartBtInt);
		    qfSession_.SetResponder(responder_);
		    return true;
	    }

        public void HandleException(Session quickFixSession, System.Exception cause, TcpClient client)
        {
            bool disconnectNeeded = true;
            string reason = cause.Message;

            System.Exception realCause = cause;
            /** TODO
            if(cause is FIXMessageDecoder.DecodeError && cause.InnerException != null)
                realCause = cause.getCause();
            */
            if (realCause is System.Net.Sockets.SocketException)
            {
                if (quickFixSession != null && quickFixSession.IsEnabled)
                    reason = "Socket exception (" + client.Client.RemoteEndPoint + "): " + cause.Message;
                else
                    reason = "Socket (" + client.Client.RemoteEndPoint + "): " + cause.Message;
                disconnectNeeded = true;
            }
            /** TODO
            else if(realCause is FIXMessageDecoder.CriticalDecodeError)
            {
                reason = "Critical protocol codec error: " + cause;
                disconnectNeeded = true;
            }
            */
            else if(realCause is MessageParseError)
            {
                reason = "Protocol handler exception: " + cause;
                if (quickFixSession == null)
                    disconnectNeeded = true;
                else
                    disconnectNeeded = false;
            }
            else
            {
                reason = cause.ToString();
                disconnectNeeded = false;
            }

            this.Log("SocketReader Error: " + reason);

            if (disconnectNeeded)
            {
                if (null != quickFixSession && quickFixSession.HasResponder)
                    quickFixSession.Disconnect(reason);
                else
                    DisconnectClient(client);
            }
        }

        /// <summary>
        /// FIXME do proper logging
        /// </summary>
        /// <param name="s"></param>
        private void Log(string s)
        {
            responder_.Log(s);
        }
    }
}