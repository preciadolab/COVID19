#!/usr/bin/python
#
# A library for finding the optimal dirichlet prior from counts
# By: Max Sklar
# @maxsklar
# https://github.com/maxsklar

# Copyright 2013 Max Sklar

import math
import random
import pdb
import numpy as np
import scipy.special as mathExtra

def digamma(x): return float(mathExtra.psi(x))
def trigamma(x): return float(mathExtra.polygamma(1, x))


#Find an initial estimator for the prior Alpha to iterate from
def getInitAlphas(samples_list):
    #compute row weights
    arr_stack = [ arr/np.sum(arr) for arr in samples_list if np.sum(arr)>0]
    if len(arr_stack) == 0:
        return(np.array([0]*168).reshape(1,168))
    else:
        probs_array = np.vstack(arr_stack)
        return(np.sum(probs_array, axis = 0))


#Find the log probability that we see a certain set of data
# give our prior.mate d
def dirichLogProb(priorList, data):
    K = data.K
    total = 0.0
    for k in range(0, K):
        for i in range(0, len(data.U[k])):
            total += data.U[k][i]*math.log(priorList[k] + i)

    sumPrior = sum(priorList)
    for i in range(0, len(data.V)):
        total -= data.V[i] * math.log(sumPrior + i)

    return total

#Gives the derivative with respect to the prior.  This will be used to adjust the loss
def priorGradient(priorList, data):
    K = data.K
    
    termToSubtract = 0
    for i in range(0, len(data.V)):
        termToSubtract += float(data.V[i]) / (sum(priorList) + i)
    
    retVal = [0]*K
    for j in range(0, K):
        for i in range(0, len(data.U[j])):
            retVal[j] += float(data.U[j][i]) / (priorList[j] + i)
    
    for j in range(0, K):
        retVal[j] -= termToSubtract
        
    return retVal

#The hessian is actually the sum of two matrices: a diagonal matrix and a constant-value matrix.
#We'll write two functions to get both
def priorHessianConst(priorList, data):
    total = 0
    for i in range(0, len(data.V)):
        total += float(data.V[i]) / (sum(priorList) + i)**2
    return total

def priorHessianDiag(priorList, data):
    K = len(data.U)
    retVal = [0]*K
    for k in range(0, K):
        for i in range(0, len(data.U[k])):
            retVal[k] -= data.U[k][i] / (priorList[k] + i)**2

    return retVal

    
# Compute the next value to try here
# http://research.microsoft.com/en-us/um/people/minka/papers/dirichlet/minka-dirichlet.pdf (eq 18)
def getPredictedStep(hConst, hDiag, gradient):
    K = len(gradient)
    numSum = 0.0
    for i in range(0, K):
        numSum += gradient[i] / hDiag[i]

    denSum = 0.0
    for i in range(0, K): denSum += 1.0 / hDiag[i]

    b = numSum / ((1.0/hConst) + denSum)

    retVal = [0]*K
    for i in range(0, K): retVal[i] = (b - gradient[i]) / hDiag[i]
    return retVal

# Uses the diagonal hessian on the log-alpha values 
def getPredictedStepAlt(hConst, hDiag, gradient, alphas):
    K = len(gradient)
    retVal = [0]*K

    denominators = [(gradient[k] - alphas[k]*hDiag[k]) for k in range(0, K)]
    for k in range(0, K):
        if (denominators[k] == 0): return retVal

    Z = 0
    for k in range(0, K):
        Z += alphas[k] / denominators[k]
    Z *= hConst

    Ss = [0]*K
    for k in range(0, K):
        Ss[k] = 1.0 / denominators[k] / (1 + Z)
    S = sum(Ss)

    for i in range(0, K): 
        retVal[i] = gradient[i] / denominators[i] * (1 - hConst * alphas[i] * S)

    return retVal

#The priors and data are global, so we don't need to pass them in
def getTotalLoss(trialPriors, data):
    return -1*dirichLogProb(trialPriors, data)
    
def predictStepUsingHessian(gradient, priors, data):
    totalHConst = priorHessianConst(priors, data)
    totalHDiag = priorHessianDiag(priors, data)
    return getPredictedStep(totalHConst, totalHDiag, gradient)
    
def predictStepLogSpace(gradient, priors, data):
    totalHConst = priorHessianConst(priors, data)
    totalHDiag = priorHessianDiag(priors, data)
    return getPredictedStepAlt(totalHConst, totalHDiag, gradient, priors)
    

# Returns whether it's a good step, and the loss    
def testTrialPriors(trialPriors, data):
    for alpha in trialPriors: 
        if alpha <= 0: 
            return float("inf")
        
    return getTotalLoss(trialPriors, data)
    
def sqVectorSize(v):
    s = 0
    for i in range(0, len(v)): s += v[i] ** 2
    return s

class CompressedRowData:
    def __init__(self, K):
        self.K = K
        self.V = []
        self.U = []
        for k in range(0, K): self.U.append([])
        
    def appendRow(self, row, weight):
        if (len(row) != self.K): stop("row must have K=" + str(self.K) + " counts")
        
        for k in range(0, self.K): 
            for j in range(0, row[k]):
                if (len(self.U[k]) == j): self.U[k].append(0)
                self.U[k][j] += weight
            
        for j in range(0, sum(row)):
            if (len(self.V) == j): self.V.append(0)
            self.V[j] += weight
    

def findDirichletPriors(data, initAlphas, iterations):
    priors = initAlphas

    # Let the learning begin!!
    #Only step in a positive direction, get the current best loss.
    currentLoss = getTotalLoss(priors, data)

    gradientToleranceSq = 2 ** -10
    learnRateTolerance = 2 ** -20

    count = 0
    while(count < iterations):
        count += 1
        
        #Get the data for taking steps
        gradient = priorGradient(priors, data)
        gradientSize = sqVectorSize(gradient) 
        print("Iteration: %s Loss: %s ,Priors: %s, Gradient Size: %s" % (count, currentLoss, priors, gradientSize))
        
        if (gradientSize < gradientToleranceSq):
            print("Converged with small gradient")
            return priors
        
        trialStep = predictStepUsingHessian(gradient, priors, data)
        
        #First, try the second order method
        trialPriors = [0]*len(priors)
        for i in range(0, len(priors)): trialPriors[i] = priors[i] + trialStep[i]
        
        #TODO: Check for taking such a small step that the loss change doesn't register (essentially converged)
        #  Fix by ending
        
        loss = testTrialPriors(trialPriors, data)
        if loss < currentLoss:
            currentLoss = loss
            priors = trialPriors
            continue
        
        trialStep = predictStepLogSpace(gradient, priors, data)
        trialPriors = [0]*len(priors)
        for i in range(0, len(priors)): 
            try:
                trialPriors[i] = priors[i] * math.exp(trialStep[i])
            except:
                trialPriors[i] = priors[i]
        loss = testTrialPriors(trialPriors, data)

        #Step in the direction of the gradient until there is a loss improvement
        learnRate = 1.0
        while loss > currentLoss:
            learnRate *= 0.9
            trialPriors = [0]*len(priors)
            for i in range(0, len(priors)): trialPriors[i] = priors[i] + gradient[i]*learnRate
            loss = testTrialPriors(trialPriors, data)

        if (learnRate < learnRateTolerance):
            prior("Converged with small learn rate")
            return priors

        currentLoss = loss
        priors = trialPriors
        
    print("Reached max iterations")
    return priors