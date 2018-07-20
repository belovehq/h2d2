(ns h2d2.test-tcp
  (:require [clojure.test :refer :all]
            [h2d2.main :refer [h2d2]]
            [h2d2.tcp :as tcp]
            [h2d2.test-fixtures :as fixtures]))


(use-fixtures :each fixtures/with-initialisation)

(deftest tcp-functions
  (testing "Test scenario using functions in the h2d2.tcp namespace."
    ; check status
    (is (= (tcp/status) {}))
    ;stop and check status
    (is (nil? (tcp/stop!)))
    (is (= (tcp/status) {}))
    ;start and check status
    (is (= org.h2.tools.Server (class (tcp/start! nil))))
    (is (let [{:keys [status port]} (tcp/status)]
          (and (= status :started) (int? port))))
    ; stop and check same port
    (is (let [{oldport :port} (tcp/status)
              _ (tcp/stop!)
              {:keys [status port]} (tcp/status)]
          (and (= status :stopped) (int? port) (= oldport port))))
    ; start and check same port
    (is (let [{oldport :port} (tcp/status)
              _ (tcp/start! nil)
              {:keys [status port]} (tcp/status)]
          (and (= status :started) (int? port) (= oldport port))))
    ; stop and restart on a set port
    (is (let [_ (tcp/stop!)
              _ (tcp/start! 58641)
              {:keys [status port]} (tcp/status)]
          (and (= status :started) (= 58641 port))))
    ; call start without specific port when already started
    (is (let [_ (tcp/start! nil)
              {:keys [status port]} (tcp/status)]
          (and (= status :started) (= 58641 port))))
    ; change port without stopping
    (is (let [_ (tcp/start! 58642)
              {:keys [status port]} (tcp/status)]
          (and (= status :started) (= 58642 port))))
    ; stop and check same port
    (is (let [{oldport :port} (tcp/status)
              _ (tcp/stop!)
              {:keys [status port]} (tcp/status)]
          (and (= status :stopped) (int? port) (= oldport port))))
    ; stop again and check same port
    (is (let [{oldport :port} (tcp/status)
              _ (tcp/stop!)
              {:keys [status port]} (tcp/status)]
          (and (= status :stopped) (int? port) (= oldport port))))
    ))

(deftest h2d2-function
  (testing "Test scenario using the h2d2 function."
    ; initial status
    (is (= (h2d2) "The database is closed. There is no TCP server yet."))
    ;stop
    (is (= (h2d2 :tcp :stop) "The database is closed. There is no TCP server yet."))
    ;start
    (is (let [msg (h2d2 :tcp :start)
              {port :port} (tcp/status)]
          (and (int? port)
               (= msg (str "The database is closed. The TCP server is running on port "
                           port ".")))))
    ; stop and check same port
    (is (let [{port :port} (tcp/status)]
          (= (h2d2 :tcp :stop)
             (str "The database is closed. The TCP server is stopped and will restart on port "
                  port "."))))
    ; start and check same port
    (is (let [{port :port} (tcp/status)]
          (= (h2d2 :tcp :start)
             (str "The database is closed. The TCP server is running on port "
                  port "."))))
    ; stop and restart on a set port
    (is (let [_ (tcp/stop!)]
          (= (h2d2 :port 58641)
             "The database is closed. The TCP server is running on port 58641.")))
    ; call start without specific port when already started
    (is (= (h2d2 :tcp :start)
           "The database is closed. The TCP server is running on port 58641."))
    ; change port without stopping
    (is (= (h2d2 :port 58642)
           "The database is closed. The TCP server is running on port 58642."))
    ; stop and check same port
    (is (= (h2d2 :tcp :stop)
           "The database is closed. The TCP server is stopped and will restart on port 58642."))
    ; stop again and check same port
    (is (= (h2d2 :tcp :stop)
           "The database is closed. The TCP server is stopped and will restart on port 58642."))
    ))

(comment
  (run-tests)
  )