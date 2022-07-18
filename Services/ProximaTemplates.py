"""Шаблоны данных для запросов к API"""
import json
from Services.ProximaHelpers import *


def recent_hospitals(skip=0, nmax=1000, update=0):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.hospitals(True),
            "first": nmax,
            "skip": skip,
            "filter": {"last_update": update},
            "include": [{"method": ProximaMethods.hospitals(), "include":
                [{"method": ProximaMethods.addresses()}, {"method": ProximaMethods.phones()}]}]
        }}, separators=(",", ":"))


def hospitals(skip=0, nmax=1000):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.hospitals(),
            "first": nmax,
            "skip": skip,
            "include":
                [{"method": ProximaMethods.addresses()}, {"method": ProximaMethods.phones()}]
        }}, separators=(",", ":"))


def types(skip=0, nmax=1000):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.types(),
            "skip": skip,
            "first": nmax
        }}, separators=(",", ":"))


def recent_persons(skip=0, nmax=1000, update=0):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.persons(True),
            "first": nmax,
            "skip": skip,
            "filter": {"last_update": update},
            "include": [{"method": ProximaMethods.persons(), "include":
                    [{"method": ProximaMethods.specialists()}, {"method": ProximaMethods.jobs()}]}]
            }}, separators=(",", ":"))


def persons(skip=0, nmax=1000):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.persons(),
            "first": nmax,
            "skip": skip,
            "include":
                [{"method": ProximaMethods.specialists()}, {"method": ProximaMethods.jobs()}]
        }}, separators=(",", ":"))


def recent_jobs(skip=0, nmax=1000, update=0):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.jobs(True),
            "first": nmax,
            "skip": skip,
            "filter": {"last_update": update},
            "include": [{"method": ProximaMethods.jobs()}]
        }}, separators=(",", ":"))


def jobs(skip=0, nmax=1000):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.jobs(),
            "first": nmax,
            "skip": skip,
        }}, separators=(",", ":"))


def posts(skip=0, nmax=1000):
    return json.dumps(
        {"query": {
            "method": ProximaMethods.posts(),
            "skip": skip,
            "first": nmax
        }}, separators=(",", ":"))


def awaited(object_id):
    return json.dumps({"query": {
        "method": ProximaMethods.awaited(),
        "object_id": object_id
    }}, separators=(",", ":"))
