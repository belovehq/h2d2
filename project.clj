(defproject h2d2 "0.1.0-SNAPSHOT"
  :description "H2 driven data visualisation droid for Clojure data scientists."
  :url "https://github.com/belovehq/h2d2"
  :license {:name "MIT License"
            :url  "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.h2database/h2 "1.4.195"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [net.mikera/vectorz-clj "0.47.0"]
                 [net.mikera/core.matrix "0.62.0"]]
  :source-paths ["src"]
  :test-paths ["test"]
  :profiles {:dev {:resource-paths ["testresources"]}})
