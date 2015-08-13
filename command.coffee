CSV = require 'csv'
_ = require 'lodash'
data = require './data.json'

OPTIONS = {
  delimiter: '\t'
}

class Command
  constructor: ->
    @columns = {}

  normalizedData: =>
    @generateColumns()

    dataRows = _.map data, (datum) =>
      row = [datum.key_as_string, datum.doc_count]

      _.each @columns, (index) =>
        row[index] = 0

      _.map datum.group_by_topic.buckets, (bucket) =>
        return unless isNaN bucket.key
        row[@indexForTopic bucket.key] = bucket.doc_count

      row

    headerRow = @generateHeaderRow()

    dataRows.unshift headerRow
    dataRows

  indexForTopic: (topic) =>
    return @columns[topic] if @columns[topic]?

    if _.isEmpty @columns
      @columns[topic] = 2
      return 2

    newIndex = 1 + _.max _.values @columns
    @columns[topic] = newIndex
    return newIndex

  generateColumns: =>
    _.each data, (datum) =>
      _.each datum.group_by_topic.buckets, (bucket) =>
        return unless isNaN bucket.key
        @indexForTopic bucket.key

  generateHeaderRow: =>
    row = ['Time', 'Total']
    _.each @columns, (index, topic) =>
      row[index] = topic
    row

  run: =>
    normalizedData = @normalizedData()
    CSV.stringify normalizedData, OPTIONS, (err, dataCSV) =>
      console.log dataCSV

command = new Command()
command.run()
