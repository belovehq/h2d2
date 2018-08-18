(ns h2d2.tcp)


; not used for atomic change but as a simple container for the tcp server,
; and locking object in functions suffixed with an exclamation mark i.e. stop! start! status-message!
(defonce h2server (atom nil))


(defn port
  "Returns the port of the TCP server"
  [server]
  (if server (.getPort server)))


(defn status
  "Returns the status of the TCP server as a map:
  :status = :stopped or :started
  :port = server port
  Returns an empty map if the server hasn't been instantiated yet."
  []
  (if @h2server
    {:status (if (= (.getStatus @h2server) "Not started")
               :stopped
               :started)
     :port   (.getPort @h2server)}
    {}))

(defn port?
  "check whether p is a valid port"
  [p]
  (and (integer? p) (> p 1023) (< p 65536)))


(defn need-new-server?
  "Checks whether a new server should be spawned given the port p0 of the current server and new port p
  (nil if the server should allocate itself a port automatically)."
  [p0 p]
  (or (and
        (not (port? p0))
        (or (nil? p) (port? p)))
      (and
        (not= p p0)
        (port? p))))


(defn stop!
  "Stops the TCP server."
  []
  (locking h2server
    (if @h2server (.stop @h2server))))


(defn start!
  "Starts the TCP server on port p (nil if the server should allocate itself a port automatically)."
  [p]
  (locking h2server
    (if (need-new-server? (port @h2server) p)
      (do (if @h2server (.stop @h2server))
          (->> (if p ["-tcpPort" (str p)] [])
               (into-array java.lang.String)
               (org.h2.tools.Server/createTcpServer)
               (reset! h2server))))
    (if (= (.getStatus @h2server) "Not started") (.start @h2server)
                                                 @h2server)))


