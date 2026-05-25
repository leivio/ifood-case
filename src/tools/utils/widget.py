# Databricks notebook source
class Widget:

    @staticmethod
    def exists(name):
        try:
            dbutils.widgets.get(name)
        except:
            return False
        else:
            return True

    @staticmethod
    def get_or_else(name, default):
        if (Widget.exists(name)):
            return dbutils.widgets.get(name)
        else:
            return default
