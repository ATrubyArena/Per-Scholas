nums1 = [1, 3]
nums2 = [2, 4]
def findMedianSortedArrays(nums1, nums2):
    merged_arr = nums1 + nums2
    merged_arr.sort()
    median_place = len(merged_arr)//2
    if len(merged_arr) % 2 == 1:
        return merged_arr[median_place]
    elif len(merged_arr) % 2 == 0:
        median_value = (merged_arr[median_place] + merged_arr[median_place - 1])/2
        return median_value

print(findMedianSortedArrays(nums1, nums2))

